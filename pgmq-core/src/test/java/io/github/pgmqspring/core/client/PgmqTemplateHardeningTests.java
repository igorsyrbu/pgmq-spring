/*
 * Copyright 2026 the pgmq-spring authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.github.pgmqspring.core.client;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqException;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.QueueNotFoundException;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.convert.PayloadConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Client edge cases: duration rounding, per-message conversion, malformed headers, error
 * translation and time-zone independence.
 */
class PgmqTemplateHardeningTests {

    record Order(String id, int quantity) {
    }

    private static PgmqTemplate pgmq;

    private static JdbcTemplate jdbc;

    @BeforeAll
    static void setUp() {
        pgmq = new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());
        jdbc = pgmq.getJdbcTemplate();
    }

    private static String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        return queue;
    }

    @Test
    void fractionalSecondsRoundUpAndHugeValuesSaturate() {
        assertThat(PgmqTemplate.seconds(Duration.ZERO)).isZero();
        assertThat(PgmqTemplate.seconds(Duration.ofSeconds(-5))).isZero();
        assertThat(PgmqTemplate.seconds(Duration.ofMillis(1))).isEqualTo(1);
        assertThat(PgmqTemplate.seconds(Duration.ofMillis(500))).isEqualTo(1);
        assertThat(PgmqTemplate.seconds(Duration.ofMillis(1500))).isEqualTo(2);
        assertThat(PgmqTemplate.seconds(Duration.ofSeconds(30))).isEqualTo(30);
        assertThat(PgmqTemplate.seconds(Duration.ofDays(365L * 1000))).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void aSubSecondVisibilityTimeoutStillHidesTheMessage() {
        String queue = newQueue("subsecond_vt");
        pgmq.send(queue, Map.of("n", 1));

        assertThat(pgmq.read(queue, ReadOptions.defaults().visibilityTimeout(Duration.ofMillis(500)))).hasSize(1);
        // Truncated to zero seconds, the message would be readable again immediately.
        assertThat(pgmq.read(queue, ReadOptions.defaults())).isEmpty();
    }

    @Test
    void conversionHappensAfterTheReadAndNamesTheMessage() {
        String queue = newQueue("convert_after");
        long bad = pgmq.sendRaw(queue, "\"not an order\"", SendOptions.none());
        pgmq.send(queue, new Order("a", 1));

        assertThatExceptionOfType(PayloadConversionException.class)
                .isThrownBy(() -> pgmq.read(queue, ReadOptions.batch(10), Order.class))
                .withMessageContaining("Message " + bad + " on queue '" + queue + "'");

        // Reading raw and converting per message is the robust path the container takes.
        jdbc.update("update pgmq.q_" + queue + " set vt = now()");
        List<PgmqMessage<String>> raw = pgmq.read(queue, ReadOptions.batch(10));
        assertThat(raw).hasSize(2);
        assertThat(pgmq.convert(raw.get(1), Order.class).payload()).isEqualTo(new Order("a", 1));
        assertThatExceptionOfType(PayloadConversionException.class)
                .isThrownBy(() -> pgmq.convert(raw.get(0), Order.class));
    }

    @Test
    void headersThatAreNotAnObjectDoNotMakeTheMessageUnreadable() {
        String queue = newQueue("odd_headers");
        jdbc.queryForObject("select pgmq.send(?::text, '{\"n\":1}'::jsonb, '[1,2]'::jsonb, 0)", Long.class, queue);

        PgmqMessage<String> message = pgmq.read(queue, ReadOptions.defaults()).get(0);
        assertThat(message.headers()).isNull();
        assertThat(message.rawPayload()).isEqualTo("{\"n\": 1}");
    }

    @Test
    void aMissingQueueIsReportedAsQueueNotFoundOnEveryOperation() {
        String missing = "definitely_missing";
        assertThatExceptionOfType(QueueNotFoundException.class)
                .isThrownBy(() -> pgmq.read(missing, ReadOptions.defaults()));
        assertThatExceptionOfType(QueueNotFoundException.class)
                .isThrownBy(() -> pgmq.send(missing, Map.of("n", 1)));
        assertThatExceptionOfType(QueueNotFoundException.class)
                .isThrownBy(() -> pgmq.setVisibilityTimeout(missing, 1L, Duration.ofSeconds(1)));
        assertThat(QueueNotFoundException.class).isAssignableTo(PgmqException.class);
    }

    @Test
    void aMissingArchiveTableIsNotReportedAsAMissingQueue() {
        String queue = newQueue("no_archive");
        long id = pgmq.send(queue, Map.of("n", 1));
        // On PGMQ 1.5.x the archive table is an extension member and must be released first.
        jdbc.execute("do $$ begin execute 'alter extension pgmq drop table pgmq.a_" + queue + "'; "
                + "exception when others then null; end $$");
        jdbc.execute("drop table pgmq.a_" + queue);

        assertThatExceptionOfType(PgmqException.class)
                .isThrownBy(() -> pgmq.archive(queue, id))
                .isNotInstanceOf(QueueNotFoundException.class)
                .withMessageContaining("missing although the queue itself exists");
        // The queue itself is untouched and still usable.
        assertThat(pgmq.delete(queue, id)).isTrue();
    }

    @Test
    void createsPartitionedQueuesThroughPgPartman() {
        Boolean available = jdbc.queryForObject(
                "select exists(select 1 from pg_available_extensions where name = 'pg_partman')", Boolean.class);
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.TRUE.equals(available), "pg_partman is not installed");
        jdbc.execute("create extension if not exists pg_partman");
        String queue = PgmqContainerSupport.uniqueQueueName("partitioned");

        pgmq.createPartitionedQueue(queue, "10000", "100000");
        long id = pgmq.send(queue, Map.of("n", 1));

        assertThat(pgmq.listQueues()).filteredOn((info) -> info.name().equals(queue))
                .singleElement().satisfies((info) -> assertThat(info.partitioned()).isTrue());
        assertThat(pgmq.read(queue, ReadOptions.defaults())).extracting(PgmqMessage::id).containsExactly(id);
    }

    @Test
    void deliverAtIsExactRegardlessOfTheJvmTimeZone() {
        String queue = newQueue("deliver_at");
        java.time.Instant at = java.time.Instant.now().plusSeconds(3600).truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        java.util.TimeZone original = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Pacific/Chatham"));
            pgmq.send(queue, Map.of("n", 1), SendOptions.none().deliverAt(at));
        }
        finally {
            java.util.TimeZone.setDefault(original);
        }
        OffsetDateTime vt = jdbc.queryForObject("select vt from pgmq.q_" + queue, OffsetDateTime.class);
        assertThat(vt.toInstant()).isEqualTo(at);
    }

    // --- setVisibleAt on every supported version ---------------------------------------------

    /** A client that believes PGMQ lacks the timestamptz overload of set_vt, as 1.5.x does. */
    private static PgmqTemplate withoutTimestampSetVt() {
        return new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter()) {
            @Override
            public PgmqCapabilities capabilities() {
                PgmqCapabilities c = super.capabilities();
                return new PgmqCapabilities(c.version(), c.installedAsExtension(), c.messageHeaders(),
                        c.lastReadAt(), c.popWithQuantity(), c.setVisibilityTimeoutBatch(), false,
                        c.topicRouting(), c.groupedReads(), c.insertNotify());
            }
        };
    }

    @Test
    void setVisibleAtWorksWithAndWithoutTheTimestamptzOverload() {
        for (PgmqTemplate client : List.of(pgmq, withoutTimestampSetVt())) {
            String queue = newQueue("visible_at");
            long single = client.send(queue, Map.of("n", 1));
            long first = client.send(queue, Map.of("n", 2));
            long second = client.send(queue, Map.of("n", 3));
            java.time.Instant target = java.time.Instant.now().plusSeconds(120);

            client.setVisibleAt(queue, single, target);
            client.setVisibleAt(queue, List.of(first, second), target);

            List<OffsetDateTime> vts = jdbc.queryForList("select vt from pgmq.q_" + queue, OffsetDateTime.class);
            assertThat(vts).hasSize(3).allSatisfy((vt) -> assertThat(Duration.between(target, vt.toInstant()).abs())
                    .as("vt lands on the requested instant, to the second").isLessThanOrEqualTo(Duration.ofSeconds(2)));
            assertThat(client.read(queue, ReadOptions.batch(10))).isEmpty();

            // An instant in the past makes the messages visible right away.
            client.setVisibleAt(queue, List.of(single, first, second), java.time.Instant.now().minusSeconds(60));
            assertThat(client.read(queue, ReadOptions.batch(10))).hasSize(3);
        }
    }

    // --- dropQueue ------------------------------------------------------------------------------

    @Test
    void droppingAQueueDropsItsArchiveToo() {
        String queue = newQueue("drop_archive");
        long id = pgmq.send(queue, Map.of("n", 1));
        assertThat(pgmq.archive(queue, id)).isTrue();

        assertThat(pgmq.dropQueue(queue)).isTrue();

        assertThat(jdbc.queryForObject("select to_regclass(?)::text", String.class, "pgmq.q_" + queue)).isNull();
        assertThat(jdbc.queryForObject("select to_regclass(?)::text", String.class, "pgmq.a_" + queue)).isNull();
        assertThat(pgmq.queueExists(queue)).isFalse();
    }

    // --- raw JSON batches -----------------------------------------------------------------------

    @Test
    void aRawDocumentThatIsNotOneJsonValueFailsTheBatchInsteadOfBecomingSeveralMessages() {
        String queue = newQueue("raw_smuggling");

        assertThatExceptionOfType(PgmqException.class)
                .isThrownBy(() -> pgmq.sendRawBatch(queue, List.of("1,2", "3"), SendOptions.none()))
                .withMessageContaining("not a single valid JSON document");
        assertThatExceptionOfType(PgmqException.class)
                .isThrownBy(() -> pgmq.sendMessages(queue, List.of(
                        OutboundMessage.ofJson("{\"a\":1}"),
                        OutboundMessage.ofJson("{\"a\":1},{\"injected\":true}"))));
        assertThatExceptionOfType(PgmqException.class)
                .isThrownBy(() -> pgmq.sendRawBatch(queue, List.of("{\"unterminated\": "), SendOptions.none()));

        assertThat(pgmq.metrics(queue).totalMessages()).as("nothing was enqueued").isZero();
    }

    @Test
    void rawDocumentsSurviveTheBatchEncodingExactly() throws Exception {
        String queue = newQueue("raw_roundtrip");
        List<String> documents = List.of(
                "{\"quote\":\"she said \\\"hi\\\"\",\"slash\":\"a\\\\b\",\"nl\":\"line1\\nline2\"}",
                "{\"unicode\":\"żółć 日本 \\u00e9 😀\",\"tab\":\"a\\tb\",\"ctrl\":\"\\u0001\"}",
                "[1, 2.5, true, null, {\"nested\": [\"x\"]}]",
                "\"just a string\"",
                "42",
                "{\n  \"pretty\": \"printed\"\n}");

        List<Long> ids = pgmq.sendRawBatch(queue, documents, SendOptions.none());

        assertThat(ids).hasSize(documents.size());
        tools.jackson.databind.ObjectMapper mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        List<PgmqMessage<String>> read = pgmq.read(queue, ReadOptions.batch(10));
        assertThat(read).hasSize(documents.size());
        for (int i = 0; i < documents.size(); i++) {
            assertThat(mapper.readTree(read.get(i).rawPayload())).isEqualTo(mapper.readTree(documents.get(i)));
        }
    }

    @Test
    void jsonStringArrayEncodingEscapesEverythingJsonRequires() {
        assertThat(PgmqTemplate.jsonStringArrayOf(List.of("a\"b", "c\\d", "\n\r\t\b\f", "\u0001", "é")))
                .isEqualTo("[\"a\\\"b\",\"c\\\\d\",\"\\n\\r\\t\\b\\f\",\"\\u0001\",\"é\"]");
        assertThat(PgmqTemplate.jsonStringArrayOf(List.of())).isEqualTo("[]");
    }

    @Test
    void rejectsNonPositiveBatchSizesAndPopCountsBeforeReachingTheDatabase() {
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.batch(0)).withMessageContaining("batchSize must be at least 1");
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.defaults().batchSize(-1));
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> pgmq.pop("never_created_q", 0, String.class))
                .withMessageContaining("count must be at least 1");
    }
}
