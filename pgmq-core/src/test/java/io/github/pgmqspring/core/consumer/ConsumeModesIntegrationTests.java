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

package io.github.pgmqspring.core.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.RecordingOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.client.SendOptions;

import static io.github.pgmqspring.core.PgmqContainerSupport.countRows;
import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/** {@link ConsumeMode#TRANSACTIONAL_POP} and {@link ConsumeMode#POP}. */
class ConsumeModesIntegrationTests {

    record Order(String id, int quantity) {
    }

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    private static JdbcTemplate jdbc;

    @RegisterExtension
    final ListenerContainers containers = new ListenerContainers();

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
        transactionManager = new DataSourceTransactionManager(PgmqContainerSupport.dataSource());
        jdbc = PgmqContainerSupport.jdbc();
    }

    private static String newTable(String queue) {
        String table = "public." + queue + "_writes";
        jdbc.execute("create table " + table + " (order_id text not null, attempt int not null)");
        return table;
    }

    private static ConsumerOptions.Builder transactionalPop() {
        return ConsumerOptions.builder()
                .consumeMode(ConsumeMode.TRANSACTIONAL_POP)
                .transactional(true)
                .transactionTimeout(Duration.ofSeconds(20))
                .pollDelay(Duration.ofMillis(50))
                .maxPollDelay(Duration.ofMillis(100));
    }

    @Test
    void aCommittedHandlerRemovesTheMessageWithOnePopAndNothingElse() {
        String queue = newQueue("txpop_commit");
        String table = newTable(queue);
        RecordingOperations recording = new RecordingOperations(pgmq);
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(recording.proxy(), queue, Order.class)
                .options(transactionalPop().build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    jdbc.update("insert into " + table + " values (?, ?)", message.payload().id(), message.readCount());
                })
                .build());
        pgmq.sendBatch(queue, List.of(new Order("a", 1), new Order("b", 2), new Order("c", 3)));

        await().atMost(Duration.ofSeconds(20)).until(() -> countRows(table) == 3);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
        assertThat(readCounts).containsOnly(1);
        // One pop per message: no read updating it first, no delete afterwards.
        assertThat(recording.calls()).doesNotContain("read", "delete", "archive", "setVisibilityTimeout")
                .filteredOn("pop"::equals).hasSizeGreaterThanOrEqualTo(3);
        assertThat(countRows("pgmq.a_" + queue)).isZero();
    }

    @Test
    void aRolledBackHandlerLeavesTheMessageCountedAndDelayed() {
        String queue = newQueue("txpop_rollback");
        String table = newTable(queue);
        CountDownLatch failed = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(transactionalPop().retryDelay(Duration.ofSeconds(3)).build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    jdbc.update("insert into " + table + " values (?, ?)", message.payload().id(), message.readCount());
                    if (message.readCount() == 1) {
                        failed.countDown();
                        throw new IllegalStateException("fails once");
                    }
                })
                .build());
        pgmq.send(queue, new Order("a", 1));

        await().atMost(Duration.ofSeconds(10)).until(() -> failed.getCount() == 0);
        // The row is back, its attempt counted in read_ct and its retry delay in vt.
        await().atMost(Duration.ofSeconds(5)).until(() -> Integer.valueOf(1).equals(jdbc.queryForObject(
                "select read_ct from pgmq.q_" + queue, Integer.class)));
        assertThat(jdbc.queryForObject("select vt > clock_timestamp() + interval '1 second' from pgmq.q_" + queue,
                Boolean.class)).isTrue();
        await().atMost(Duration.ofSeconds(20)).until(() -> readCounts.contains(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(jdbc.queryForList("select attempt from " + table, Integer.class)).containsExactly(2);
    }

    @Test
    void aMessageIsDeadLetteredOnceItsAttemptsAreExhausted() {
        String queue = newQueue("txpop_exhausted");
        String dlq = newQueue("txpop_exhausted_dlq");
        AtomicInteger invocations = new AtomicInteger();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(transactionalPop()
                        .retryDelay(Duration.ZERO)
                        .maxAttempts(3)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    invocations.incrementAndGet();
                    throw new IllegalStateException("always fails");
                })
                .build());
        pgmq.send(queue, new Order("a", 1));

        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(dlq).queueLength() == 1);
        assertThat(invocations).hasValue(3);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
        PgmqMessage<String> dead = pgmq.read(dlq, ReadOptions.defaults()).get(0);
        assertThat(dead.header(DeadLetterHeaders.READ_COUNT)).isEqualTo(3);
    }

    @Test
    void anUnconvertibleMessageRollsBackItsBatchAndIsHandledLikeAFailure() {
        String queue = newQueue("txpop_unconvertible");
        String dlq = newQueue("txpop_unconvertible_dlq");
        ConcurrentLinkedQueue<String> handled = new ConcurrentLinkedQueue<>();

        pgmq.sendRaw(queue, "\"not an order\"", SendOptions.none());
        pgmq.sendBatch(queue, List.of(new Order("a", 1), new Order("b", 2)));
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(transactionalPop()
                        .batchSize(10)
                        .retryDelay(Duration.ZERO)
                        .maxAttempts(2)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .build())
                .transactionManager(transactionManager)
                .batchHandler((batch) -> batch.forEach((message) -> handled.add(message.payload().id())))
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 2 && pgmq.metrics(dlq).queueLength() == 1);
        assertThat(handled).containsExactlyInAnyOrder("a", "b");
        PgmqMessage<String> dead = pgmq.read(dlq, ReadOptions.defaults()).get(0);
        assertThat(dead.rawPayload()).isEqualTo("\"not an order\"");
        assertThat(dead.header(DeadLetterHeaders.READ_COUNT)).isEqualTo(2);
        await().atMost(Duration.ofSeconds(5)).until(() -> pgmq.metrics(queue).queueLength() == 0);
    }

    @Test
    void aBatchHandlerPopsAWholeBatchInOneTransaction() {
        String queue = newQueue("txpop_batch");
        pgmq.sendBatch(queue, List.of(new Order("a", 1), new Order("b", 2), new Order("c", 3), new Order("d", 4)));
        ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(transactionalPop().batchSize(10).build())
                .transactionManager(transactionManager)
                .batchHandler((batch) -> batchSizes.add(batch.size()))
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(batchSizes).containsExactly(4);
    }

    @Test
    void aConsumerKilledMidTransactionFreesItsMessageForAnotherAtOnce() throws InterruptedException {
        String queue = newQueue("txpop_killed");
        CountDownLatch popped = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> secondConsumer = new ConcurrentLinkedQueue<>();
        try {
            this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                    .options(transactionalPop().build())
                    .transactionManager(transactionManager)
                    .handler((message) -> {
                        popped.countDown();
                        release.await(30, TimeUnit.SECONDS);
                    })
                    .build());
            pgmq.send(queue, new Order("a", 1));
            assertThat(popped.await(10, TimeUnit.SECONDS)).isTrue();
            this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                    .options(transactionalPop().build())
                    .transactionManager(transactionManager)
                    .handler((message) -> secondConsumer.add(message.readCount()))
                    .build());
            // Held by a row lock, not a lease: the second consumer cannot see it meanwhile.
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(secondConsumer::isEmpty);

            jdbc.queryForList("select pg_terminate_backend(pid) from pg_stat_activity "
                    + "where state = 'idle in transaction' and query like '%pgmq.pop%' and pid <> pg_backend_pid()");

            // No lease to wait out: the rollback makes it available again at once.
            await().atMost(Duration.ofSeconds(5)).until(() -> secondConsumer.size() == 1);
            assertThat(secondConsumer).containsExactly(1);
        }
        finally {
            release.countDown();
        }
    }

    @Test
    void popIsAtMostOnce() {
        String queue = newQueue("pop_at_most_once");
        AtomicInteger invocations = new AtomicInteger();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(ConsumerOptions.builder()
                        .consumeMode(ConsumeMode.POP)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> {
                    invocations.incrementAndGet();
                    throw new IllegalStateException("fails, and the message is gone");
                })
                .build());
        pgmq.send(queue, new Order("a", 1));

        await().atMost(Duration.ofSeconds(10)).until(() -> invocations.get() == 1);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> invocations.get() == 1);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
        assertThat(countRows("pgmq.a_" + queue)).isZero();
    }

    @Test
    void popHandsEveryMessageToABatchHandler() {
        String queue = newQueue("pop_batch");
        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2), Map.of("n", 3)));
        ConcurrentLinkedQueue<Long> handled = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .consumeMode(ConsumeMode.POP)
                        .batchSize(10)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .batchHandler((batch) -> batch.forEach((message) -> handled.add(message.id())))
                .build());

        await().atMost(Duration.ofSeconds(10)).until(() -> handled.size() == 3);
        assertThat(handled).doesNotHaveDuplicates();
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
    }

    @Test
    void rejectsCombinationsAPoppedMessageCannotSupport() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().consumeMode(ConsumeMode.TRANSACTIONAL_POP).build())
                .withMessageContaining("requires transactional=true");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> transactionalPop().groupOrdered(true).build())
                .withMessageContaining("groupOrdered");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> transactionalPop().extendLease(true).build())
                .withMessageContaining("extendLease");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> transactionalPop().longPoll(Duration.ofSeconds(5)).build())
                .withMessageContaining("longPoll");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> transactionalPop().acknowledgeMode(AcknowledgeMode.ARCHIVE).build())
                .withMessageContaining("acknowledgeMode=DELETE");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder()
                        .consumeMode(ConsumeMode.POP).batchAcknowledgements(true).build())
                .withMessageContaining("batchAcknowledgements");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().consumeMode(ConsumeMode.POP).transactional(true).build())
                .withMessageContaining("TRANSACTIONAL_POP");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().consumeMode(ConsumeMode.POP)
                        .failureAction(FailureAction.DEAD_LETTER).deadLetterQueue("dlq").build())
                .withMessageContaining("failureAction=DEAD_LETTER");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(pgmq, "orders", String.class)
                        .options(ConsumerOptions.builder().consumeMode(ConsumeMode.POP).build())
                        .acknowledgingHandler((message, acknowledgement) -> acknowledgement.acknowledge())
                        .build())
                .withMessageContaining("acknowledgingHandler");
        ConsumerOptions options = transactionalPop().build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("consumeMode=TRANSACTIONAL_POP");
        assertThat(ConsumerOptions.defaults().getConsumeMode()).isEqualTo(ConsumeMode.READ);
    }
}
