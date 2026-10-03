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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqException;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** {@link PgmqTemplate#setMaxBatchSize(Integer)}: long batches split into several statements. */
class ChunkedBatchSendIntegrationTests {

    private PgmqTemplate pgmq;

    private JdbcTemplate jdbc;

    private DataSource dataSource;

    /** The number of messages in each send_batch statement issued. */
    private final List<Integer> statementSizes = new CopyOnWriteArrayList<>();

    /** The ids each listener notification reported. */
    private final List<Integer> notifiedSizes = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        this.dataSource = PgmqContainerSupport.dataSource();
        this.pgmq = new PgmqTemplate(new StatementCountingJdbcTemplate(this.dataSource), new JacksonPayloadConverter());
        this.jdbc = new JdbcTemplate(this.dataSource);
        this.pgmq.setClientListener(new PgmqClientListener() {
            @Override
            public void onSent(String queue, List<Long> messageIds, Duration duration) {
                ChunkedBatchSendIntegrationTests.this.notifiedSizes.add(messageIds.size());
            }
        });
    }

    @AfterEach
    void clearStatements() {
        this.statementSizes.clear();
        this.notifiedSizes.clear();
    }

    /** Records how many messages each send_batch statement carried, from its payload array parameter. */
    private final class StatementCountingJdbcTemplate extends JdbcTemplate {

        StatementCountingJdbcTemplate(DataSource dataSource) {
            super(dataSource);
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, @Nullable Object... args) {
            if (sql.contains("pgmq.send_batch") && args != null && args[1] instanceof String payloads) {
                Integer size = super.queryForObject("select jsonb_array_length(?::jsonb)", Integer.class, payloads);
                ChunkedBatchSendIntegrationTests.this.statementSizes.add(size);
            }
            return super.query(sql, rowMapper, args);
        }
    }

    private String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        this.pgmq.createQueue(queue);
        return queue;
    }

    private int count(String queue) {
        Integer n = this.jdbc.queryForObject("select count(*) from pgmq.q_" + queue, Integer.class);
        return n != null ? n : 0;
    }

    private static List<Map<String, Integer>> payloads(int count) {
        return IntStream.range(0, count).mapToObj((n) -> Map.of("n", n)).toList();
    }

    @Test
    void aLongBatchIsSentInChunksAndReturnsItsIdsInOrder() {
        String queue = newQueue("chunked");
        this.pgmq.setMaxBatchSize(1000);

        List<Long> ids = this.pgmq.sendBatch(queue, payloads(2500));

        assertThat(this.statementSizes).containsExactly(1000, 1000, 500);
        assertThat(this.notifiedSizes).containsExactly(2500);
        assertThat(ids).hasSize(2500).isSorted().doesNotHaveDuplicates();
        List<Integer> sentOrder = this.jdbc.queryForList(
                "select (message->>'n')::int from pgmq.q_" + queue + " order by msg_id", Integer.class);
        assertThat(sentOrder).isEqualTo(IntStream.range(0, 2500).boxed().toList());
    }

    @Test
    void withoutAMaximumABatchIsOneStatement() {
        String queue = newQueue("unchunked");

        this.pgmq.sendBatch(queue, payloads(2500));

        assertThat(this.statementSizes).containsExactly(2500);
        assertThat(this.notifiedSizes).containsExactly(2500);
        assertThat(this.pgmq.getMaxBatchSize()).isNull();
    }

    @Test
    void aFailingChunkRollsBackTheEarlierOnesInsideTheCallersTransaction() {
        String queue = newQueue("chunked_tx");
        this.pgmq.setMaxBatchSize(1000);
        List<String> json = new ArrayList<>(payloads(2500).stream().map((p) -> "{\"n\":" + p.get("n") + "}").toList());
        json.set(2100, "{\"a\":1},{\"b\":2}");
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(this.dataSource));

        assertThatExceptionOfType(PgmqException.class).isThrownBy(() -> transaction.executeWithoutResult(
                (status) -> this.pgmq.sendRawBatch(queue, json, SendOptions.none())));

        assertThat(this.statementSizes).containsExactly(1000, 1000, 500);
        assertThat(this.notifiedSizes).isEmpty();
        assertThat(count(queue)).isZero();
    }

    @Test
    void aFailingChunkRollsBackTheEarlierOnesWithoutATransaction() {
        String queue = newQueue("chunked_autocommit");
        this.pgmq.setMaxBatchSize(1000);
        List<String> json = new ArrayList<>(payloads(2500).stream().map((p) -> "{\"n\":" + p.get("n") + "}").toList());
        json.set(2100, "{\"a\":1},{\"b\":2}");

        assertThatExceptionOfType(PgmqException.class)
                .isThrownBy(() -> this.pgmq.sendRawBatch(queue, json, SendOptions.none()));

        assertThat(this.statementSizes).containsExactly(1000, 1000, 500);
        assertThat(this.notifiedSizes).isEmpty();
        assertThat(count(queue)).isZero();
    }

    @Test
    void chunksJoinTheCallersTransaction() {
        String queue = newQueue("chunked_rollback");
        this.pgmq.setMaxBatchSize(2);
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(this.dataSource));

        transaction.executeWithoutResult((status) -> {
            this.pgmq.sendBatch(queue, payloads(5));
            status.setRollbackOnly();
        });

        assertThat(this.statementSizes).containsExactly(2, 2, 1);
        assertThat(count(queue)).isZero();
    }

    @Test
    void perMessageHeadersStayWithTheirMessagesAcrossChunks() {
        String queue = newQueue("chunked_headers");
        this.pgmq.setMaxBatchSize(2);

        List<Long> ids = this.pgmq.sendMessages(queue, IntStream.range(0, 5)
                .mapToObj((n) -> n == 3 ? OutboundMessage.of(Map.of("n", n))
                        : OutboundMessage.of(Map.of("n", n)).withHeader("seq", n))
                .toList(), SendOptions.headers(Map.of("source", "test")));

        assertThat(this.statementSizes).containsExactly(2, 2, 1);
        List<PgmqMessage<String>> read = this.pgmq.read(queue, ReadOptions.batch(10));
        assertThat(read).extracting(PgmqMessage::id).containsExactlyElementsOf(ids);
        for (int n = 0; n < 5; n++) {
            assertThat(read.get(n).rawPayload()).contains("\"n\": " + n);
            assertThat(read.get(n).header("source")).isEqualTo("test");
            assertThat(read.get(n).header("seq")).isEqualTo(n == 3 ? null : n);
        }
    }

    @Test
    void rejectsAMaximumBelowOne() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.pgmq.setMaxBatchSize(0))
                .withMessageContaining("maxBatchSize must be at least 1");
    }
}
