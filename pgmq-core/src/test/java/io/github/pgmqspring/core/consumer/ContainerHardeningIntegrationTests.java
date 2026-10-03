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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.InvalidQueueNameException;
import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.client.SendOptions;
import io.github.pgmqspring.core.convert.PayloadConversionException;

import static io.github.pgmqspring.core.PgmqContainerSupport.countRows;
import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;

/**
 * Failure modes that would lose, duplicate or wedge messages: malformed payloads, slow handlers,
 * settlements that fail, missing queues, shutdown and option validation.
 */
class ContainerHardeningIntegrationTests {

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

    @Test
    void anUnconvertibleMessageIsDeadLetteredWithoutBlockingItsBatchMates() {
        String queue = newQueue("unconvertible");
        String dlq = newQueue("unconvertible_dlq");
        ConcurrentLinkedQueue<String> handled = new ConcurrentLinkedQueue<>();

        // Sent first so it lands in the same batch as the good messages.
        pgmq.sendRaw(queue, "\"not an order\"", SendOptions.none());
        pgmq.sendBatch(queue, List.of(new Order("a", 1), new Order("b", 2), new Order("c", 3)));

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(ConsumerOptions.builder()
                        .batchSize(10)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .maxAttempts(2)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> handled.add(message.payload().id()))
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 3);
        await().atMost(Duration.ofSeconds(20))
                .until(() -> countRows("pgmq.q_" + queue) == 0 && countRows("pgmq.q_" + dlq) == 1);
        assertThat(handled).containsExactlyInAnyOrder("a", "b", "c");
        PgmqMessage<String> dead = pgmq.read(dlq, ReadOptions.defaults()).get(0);
        assertThat(dead.rawPayload()).isEqualTo("\"not an order\"");
        assertThat(dead.header(DeadLetterHeaders.EXCEPTION_TYPE)).isEqualTo(PayloadConversionException.class.getName());
    }

    @Test
    void theLeaseCoversMessagesWaitingBehindASlowOne() {
        String queue = newQueue("lease_batch");
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();

        // Three messages at 2s each take 6s, twice the 3s visibility timeout. Without a batch-wide
        // lease the second polling loop re-reads the waiting ones and they run twice. The lease is
        // refreshed every second, so a refresh delayed by a busy machine still leaves margin.
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .concurrency(2)
                        .batchSize(3)
                        .visibilityTimeout(Duration.ofSeconds(3))
                        .extendLease(true)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    Thread.sleep(2000);
                })
                .build());
        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2), Map.of("n", 3)));

        await().atMost(Duration.ofSeconds(30)).until(() -> countRows("pgmq.q_" + queue) == 0);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> readCounts.size() == 3);
        assertThat(readCounts).containsOnly(1);
    }

    @Test
    void theLeaseCoversABatchHandler() {
        String queue = newQueue("lease_batch_handler");
        ConcurrentLinkedQueue<Long> handled = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .concurrency(2)
                        .batchSize(5)
                        .visibilityTimeout(Duration.ofSeconds(3))
                        .extendLease(true)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .batchHandler((batch) -> {
                    batch.forEach((message) -> handled.add(message.id()));
                    Thread.sleep(6000);
                })
                .build());
        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2)));

        await().atMost(Duration.ofSeconds(30)).until(() -> countRows("pgmq.q_" + queue) == 0);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> handled.size() == 2);
        assertThat(handled).doesNotHaveDuplicates();
    }

    @Test
    void aLeaseRefreshNeverOverwritesARetryDelay() {
        String queue = newQueue("lease_retry");
        CountDownLatch failed = new CountDownLatch(1);

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .visibilityTimeout(Duration.ofSeconds(3))
                        .extendLease(true)
                        .retryDelay(Duration.ofSeconds(60))
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .handler((message) -> {
                    Thread.sleep(1500);
                    throw new IllegalStateException("boom");
                })
                .listener(new ConsumerListener() {
                    @Override
                    public void onFailure(String q, PgmqMessage<?> message, Duration duration, Throwable error) {
                        failed.countDown();
                    }
                })
                .build());
        pgmq.send(queue, Map.of("n", 1));

        await().atMost(Duration.ofSeconds(20)).until(() -> failed.getCount() == 0);
        // Several refresh periods later the message must still be parked for about a minute.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> {
            OffsetDateTime vt = jdbc.queryForObject("select vt from pgmq.q_" + queue, OffsetDateTime.class);
            return vt != null && vt.isAfter(OffsetDateTime.now().plusSeconds(40));
        });
    }

    @Test
    void aHandlerThatSettlesAndThenThrowsIsNotAlsoDeadLettered() {
        String queue = newQueue("ack_then_throw");
        String dlq = newQueue("ack_then_throw_dlq");
        CountDownLatch failed = new CountDownLatch(1);

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.MANUAL)
                        .maxAttempts(1)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .acknowledgingHandler((message, acknowledgement) -> {
                    acknowledgement.acknowledge();
                    throw new IllegalStateException("fails after acknowledging");
                })
                .listener(new ConsumerListener() {
                    @Override
                    public void onFailure(String q, PgmqMessage<?> message, Duration duration, Throwable error) {
                        failed.countDown();
                    }
                })
                .build());
        pgmq.send(queue, Map.of("n", 1));

        await().atMost(Duration.ofSeconds(20)).until(() -> failed.getCount() == 0);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                .until(() -> countRows("pgmq.q_" + queue) == 0 && countRows("pgmq.q_" + dlq) == 0);
    }

    @Test
    void aSettlementThatFailsFallsBackToTheFailureAction() {
        String queue = newQueue("settle_fails");
        AtomicInteger attempts = new AtomicInteger();

        // deadLetter() with no dead-letter queue throws. The message must not count as settled, so
        // the failure action applies: here, parking it for the 60s retry delay. Were it mistaken
        // for settled it would be left alone and come straight back after its 1s lease.
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.MANUAL)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ofSeconds(60))
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .acknowledgingHandler((message, acknowledgement) -> {
                    attempts.incrementAndGet();
                    acknowledgement.deadLetter("no dead-letter queue configured");
                })
                .build());
        pgmq.send(queue, Map.of("n", 1));

        await().atMost(Duration.ofSeconds(20)).until(() -> attempts.get() == 1);
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).until(() -> attempts.get() == 1);
        OffsetDateTime vt = jdbc.queryForObject("select vt from pgmq.q_" + queue, OffsetDateTime.class);
        assertThat(vt).isAfter(OffsetDateTime.now().plusSeconds(40));
    }

    @Test
    void startFailsFastWhenTheQueueOrItsDeadLetterQueueIsMissing() {
        String queue = newQueue("verify");
        assertThatIllegalStateException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(pgmq, "no_such_queue_x", String.class)
                        .handler((message) -> { })
                        .build()
                        .start())
                .withMessageContaining("'no_such_queue_x' does not exist");
        PgmqMessageListenerContainer<String> missingDlq = PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue("no_such_dlq_x")
                        .build())
                .handler((message) -> { })
                .build();
        assertThatIllegalStateException().isThrownBy(missingDlq::start)
                .withMessageContaining("dead-letter queue of '" + queue + "'");
        assertThat(missingDlq.isRunning()).isFalse();
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                        .options(ConsumerOptions.builder().deadLetterQueue(queue).build())
                        .handler((message) -> { })
                        .build())
                .withMessageContaining("its own dead-letter queue");
        // The escape hatch for queues created after the container starts.
        PgmqMessageListenerContainer<String> unchecked = PgmqMessageListenerContainer
                .builder(pgmq, "created_later_x", String.class)
                .handler((message) -> { })
                .verifyQueuesOnStart(false)
                .build();
        this.containers.start(unchecked);
        assertThat(unchecked.isRunning()).isTrue();
    }

    @Test
    void asynchronousStopReturnsImmediatelyAndCallsBackOnceDrained() throws Exception {
        String queue = newQueue("async_stop");
        CountDownLatch inHandler = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        PgmqMessageListenerContainer<String> container = this.containers.start(PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().pollDelay(Duration.ofMillis(50)).build())
                .handler((message) -> {
                    inHandler.countDown();
                    Thread.sleep(3000);
                    finished.set(true);
                })
                .build());
        pgmq.send(queue, Map.of("n", 1));
        assertThat(inHandler.await(20, TimeUnit.SECONDS)).isTrue();

        CountDownLatch stopped = new CountDownLatch(1);
        container.stop(stopped::countDown);
        // Relative rather than a wall-clock bound, so a slow machine cannot fail it: the handler is
        // still sleeping when stop(Runnable) returns, so it did not wait for the drain.
        assertThat(finished).as("stop(Runnable) returned before the in-flight handler finished").isFalse();
        assertThat(container.isRunning()).isFalse();
        assertThat(stopped.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(finished).isTrue();
    }

    @Test
    void aRunningContainerKeepsTheJvmAliveUntilItStops() {
        String queue = newQueue("keepalive");
        PgmqMessageListenerContainer<String> container = PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .handler((message) -> { })
                .build();
        container.setBeanName("keepalive-test");
        Supplier<List<Thread>> keepers = () -> Thread.getAllStackTraces().keySet().stream()
                .filter((t) -> t.getName().equals("keepalive-test-keepalive") && t.isAlive())
                .toList();

        this.containers.start(container);
        // Polling threads are daemon threads; without a non-daemon thread a consumer-only
        // application would exit right after startup.
        assertThat(keepers.get()).singleElement().satisfies((t) -> assertThat(t.isDaemon()).isFalse());

        container.stop();
        await().atMost(Duration.ofSeconds(5)).until(() -> keepers.get().isEmpty());
    }

    @Test
    void aBlockingStopWaitsForAnAsynchronousStopThatIsStillDraining() throws Exception {
        String queue = newQueue("stop_join");
        CountDownLatch inHandler = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        PgmqMessageListenerContainer<String> container = this.containers.start(PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().pollDelay(Duration.ofMillis(50)).build())
                .handler((message) -> {
                    inHandler.countDown();
                    Thread.sleep(1500);
                    finished.set(true);
                })
                .build());
        pgmq.send(queue, Map.of("n", 1));
        assertThat(inHandler.await(20, TimeUnit.SECONDS)).isTrue();

        container.stop(() -> { });
        container.stop();

        assertThat(finished).as("stop() returned only once the in-flight handler had finished").isTrue();
    }

    @Test
    void aBatchHandlerLogsWithTheQueueInTheMdc() {
        String queue = newQueue("batch_mdc");
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().batchSize(5).pollDelay(Duration.ofMillis(50)).build())
                .batchHandler((batch) -> seen.add(String.valueOf(org.slf4j.MDC.get("pgmq.queue"))))
                .build());
        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2)));

        await().atMost(Duration.ofSeconds(20)).until(() -> !seen.isEmpty());
        assertThat(seen).containsOnly(queue);
    }

    @Test
    void optionsRejectDurationsThatWouldMisbehave() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().pollDelay(Duration.ZERO).build())
                .withMessageContaining("pollDelay must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().visibilityTimeout(Duration.ZERO).build())
                .withMessageContaining("visibilityTimeout must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder()
                        .pollDelay(Duration.ofSeconds(2)).maxPollDelay(Duration.ofSeconds(1)).build())
                .withMessageContaining("maxPollDelay");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().longPoll(Duration.ofMillis(500)).build())
                .withMessageContaining("longPoll must be at least 1s");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().retryDelay(Duration.ofSeconds(-1)).build())
                .withMessageContaining("retryDelay must not be negative");
        org.assertj.core.api.Assertions.assertThatExceptionOfType(InvalidQueueNameException.class)
                .isThrownBy(() -> ConsumerOptions.builder().deadLetterQueue("bad;name").build());
        assertThat(ConsumerOptions.defaults().toString()).contains("visibilityTimeout=PT30S");
        ConsumerOptions base = ConsumerOptions.builder().concurrency(3).maxAttempts(7).extendLease(true).build();
        ConsumerOptions derived = base.toBuilder().concurrency(8).build();
        assertThat(derived.toString()).isEqualTo(base.toString().replace("concurrency=3", "concurrency=8"));
    }
}
