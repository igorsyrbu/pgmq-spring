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

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@link PgmqOperations#sendMessages}: batches whose messages carry their
 * own headers.
 */
class SendMessagesIntegrationTests {

    record Event(String userId, int seq) {
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
    void eachMessageKeepsItsOwnHeadersAndTheBatchKeepsItsOrder() {
        String queue = newQueue("per_msg_headers");

        List<Long> ids = pgmq.sendMessages(queue, List.of(
                OutboundMessage.of(new Event("u1", 1)).group("u1"),
                OutboundMessage.of(new Event("u2", 1)).group("u2").withHeader("trace", "t-2"),
                OutboundMessage.of(new Event("u3", 1))));

        assertThat(ids).hasSize(3).isSorted();
        List<PgmqMessage<Event>> read = pgmq.read(queue, ReadOptions.batch(10), Event.class);
        assertThat(read).extracting(PgmqMessage::id).containsExactlyElementsOf(ids);
        assertThat(read.get(0).headers()).isEqualTo(Map.of(FifoGroups.GROUP_HEADER, "u1"));
        assertThat(read.get(1).headers()).isEqualTo(Map.of(FifoGroups.GROUP_HEADER, "u2", "trace", "t-2"));
        assertThat(read.get(2).headers()).isNull();
        assertThat(read).extracting((m) -> m.payload().userId()).containsExactly("u1", "u2", "u3");
        // A message without headers is stored as SQL NULL, exactly like a single send without any.
        assertThat(jdbc.queryForObject("select count(*) from pgmq.q_" + queue + " where headers is null",
                Integer.class)).isEqualTo(1);
    }

    @Test
    void sharedHeadersAreDefaultsThatAMessageCanOverride() {
        String queue = newQueue("shared_headers");

        pgmq.sendMessages(queue, List.of(
                OutboundMessage.of(Map.of("n", 1)),
                OutboundMessage.of(Map.of("n", 2)).withHeader("source", "override").withHeader("extra", 1)),
                SendOptions.headers(Map.of("source", "importer", "tenant", "acme")));

        List<PgmqMessage<String>> read = pgmq.read(queue, ReadOptions.batch(10));
        assertThat(read.get(0).headers()).isEqualTo(Map.of("source", "importer", "tenant", "acme"));
        assertThat(read.get(1).headers()).isEqualTo(Map.of("source", "override", "tenant", "acme", "extra", 1));
    }

    @Test
    void preSerializedPayloadsAreStoredVerbatim() {
        String queue = newQueue("json_payloads");

        pgmq.sendMessages(queue, List.of(
                OutboundMessage.ofJson("{\"already\":\"json\"}"),
                OutboundMessage.of("{\"already\":\"json\"}")));

        List<PgmqMessage<String>> read = pgmq.read(queue, ReadOptions.batch(10));
        assertThat(read.get(0).rawPayload()).isEqualTo("{\"already\": \"json\"}");
        // Without ofJson the string is an object to serialize, so it is stored as a JSON string.
        assertThat(read.get(1).rawPayload()).isEqualTo("\"{\\\"already\\\":\\\"json\\\"}\"");
    }

    @Test
    void theDelayAppliesToTheWholeBatch() {
        String queue = newQueue("batch_delay");

        pgmq.sendMessages(queue, List.of(OutboundMessage.of(Map.of("n", 1)).group("a"),
                OutboundMessage.of(Map.of("n", 2)).group("b")), SendOptions.delayed(Duration.ofSeconds(60)));

        assertThat(pgmq.read(queue, ReadOptions.batch(10))).isEmpty();
        assertThat(jdbc.queryForObject("select min(vt) from pgmq.q_" + queue, OffsetDateTime.class))
                .isAfter(OffsetDateTime.now().plusSeconds(40));
    }

    @Test
    void oneBatchCanFeedSeveralFifoGroups() {
        assumeTrue(pgmq.capabilities().groupedReads(), "grouped reads need PGMQ 1.10.0");
        String queue = newQueue("batch_groups");

        pgmq.sendMessages(queue, List.of(
                OutboundMessage.of(new Event("u1", 1)).group("u1"),
                OutboundMessage.of(new Event("u1", 2)).group("u1"),
                OutboundMessage.of(new Event("u2", 1)).group("u2"),
                OutboundMessage.of(new Event("u2", 2)).group("u2")));

        List<PgmqMessage<Event>> first = pgmq.readGrouped(queue,
                ReadOptions.batch(10).groupStrategy(GroupReadStrategy.HEAD), Event.class);
        assertThat(first).extracting(PgmqMessage::payload)
                .containsExactlyInAnyOrder(new Event("u1", 1), new Event("u2", 1));
    }

    @Test
    void rejectsInvalidInputWithoutTouchingTheDatabase() {
        assertThat(pgmq.sendMessages("never_created_q", List.of())).isEmpty();
        assertThatIllegalArgumentException().isThrownBy(() -> OutboundMessage.of(null));
        assertThatIllegalArgumentException().isThrownBy(() -> OutboundMessage.ofJson(" "));
        assertThatIllegalArgumentException().isThrownBy(() -> OutboundMessage.of("x").group(""));
        assertThatIllegalArgumentException().isThrownBy(() -> OutboundMessage.of("x").withHeader("k", null));
    }
}
