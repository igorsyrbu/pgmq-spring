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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;

/**
 * The transactional-outbox guarantee: a send takes part in the caller's Spring transaction.
 *
 * <p>This is the property the whole library is built around, so it is tested directly rather than
 * inferred from the fact that {@code JdbcTemplate} is used.
 */
class TransactionalSendIntegrationTests {

    private static DataSource dataSource;

    private static PgmqTemplate pgmq;

    private static TransactionTemplate transactionTemplate;

    private static JdbcTemplate jdbc;

    @BeforeAll
    static void setUp() {
        dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
        jdbc = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        jdbc.execute("create table if not exists outbox_demo (id text primary key, note text)");
    }

    private String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        return queue;
    }

    @Test
    void commitPersistsBothTheRowAndTheMessage() {
        String queue = newQueue("tx_commit");

        transactionTemplate.executeWithoutResult((status) -> {
            jdbc.update("insert into outbox_demo(id, note) values (?, ?)", "tx-commit", "kept");
            pgmq.send(queue, "committed-message");
        });

        assertThat(jdbc.queryForObject(
                "select count(*) from outbox_demo where id = ?", Integer.class, "tx-commit")).isEqualTo(1);
        assertThat(pgmq.read(queue, ReadOptions.defaults())).hasSize(1);
    }

    @Test
    void rollbackLeavesNoMessageAndNoRow() {
        String queue = newQueue("tx_rollback");

        assertThatRuntimeException().isThrownBy(() -> transactionTemplate.executeWithoutResult((status) -> {
            jdbc.update("insert into outbox_demo(id, note) values (?, ?)", "tx-rollback", "discarded");
            pgmq.send(queue, "message-that-must-not-survive");
            throw new IllegalStateException("business failure after the send");
        })).withMessage("business failure after the send");

        assertThat(jdbc.queryForObject(
                "select count(*) from outbox_demo where id = ?", Integer.class, "tx-rollback")).isZero();
        assertThat(pgmq.read(queue, ReadOptions.defaults())).isEmpty();
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
    }

    @Test
    void messageIsInvisibleToOtherConnectionsUntilTheTransactionCommits() {
        String queue = newQueue("tx_isolation");

        transactionTemplate.executeWithoutResult((status) -> {
            pgmq.send(queue, "not-yet-committed");
            // A different PgmqTemplate on a *different* pooled connection must not see the
            // uncommitted row. Reading through the same template would join this transaction.
            PgmqTemplate other = new PgmqTemplate(
                    new JdbcTemplate(dataSource), new JacksonPayloadConverter());
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<List<PgmqMessage<String>>> outside =
                        executor.submit(() -> other.read(queue, ReadOptions.defaults()));
                assertThat(outside.get(10, TimeUnit.SECONDS)).isEmpty();
            }
            catch (Exception ex) {
                throw new AssertionError("concurrent read failed", ex);
            }
            finally {
                executor.shutdownNow();
            }
        });

        assertThat(pgmq.read(queue, ReadOptions.defaults())).hasSize(1);
    }

    @Test
    void rollbackOfAnAcknowledgementRestoresTheMessage() {
        String queue = newQueue("tx_ack_rollback");
        long id = pgmq.send(queue, "ack-rollback");
        pgmq.read(queue, ReadOptions.defaults());

        assertThatRuntimeException().isThrownBy(() -> transactionTemplate.executeWithoutResult((status) -> {
            pgmq.delete(queue, id);
            throw new IllegalStateException("failure after ack");
        }));

        // The delete rolled back, so the message is still in the queue.
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);
    }

    @Test
    void competingConsumersEachReceiveEveryMessageExactlyOnce() throws Exception {
        String queue = newQueue("competing");
        int messageCount = 200;
        int consumers = 8;

        List<Long> sent = pgmq.sendBatch(queue, IntStream.range(0, messageCount).boxed().toList());
        assertThat(sent).hasSize(messageCount);

        AtomicInteger received = new AtomicInteger();
        java.util.Set<Long> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.List<Long> duplicates = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(consumers);
        try {
            List<Callable<Void>> tasks = IntStream.range(0, consumers).<Callable<Void>>mapToObj((i) -> () -> {
                while (received.get() < messageCount) {
                    List<PgmqMessage<String>> batch = pgmq.read(queue, ReadOptions.batch(5));
                    if (batch.isEmpty()) {
                        break;
                    }
                    for (PgmqMessage<String> message : batch) {
                        if (!seen.add(message.id())) {
                            duplicates.add(message.id());
                        }
                        received.incrementAndGet();
                        pgmq.delete(queue, message.id());
                    }
                }
                return null;
            }).toList();
            for (Future<Void> future : executor.invokeAll(tasks, 120, TimeUnit.SECONDS)) {
                future.get();
            }
        }
        finally {
            executor.shutdownNow();
        }

        // FOR UPDATE SKIP LOCKED in pgmq.read() is what makes this hold.
        assertThat(duplicates).as("no message delivered to two consumers concurrently").isEmpty();
        assertThat(seen).hasSize(messageCount);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
    }
}
