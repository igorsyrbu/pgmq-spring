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

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for {@link PgmqMessageListenerContainer}.
 *
 * <p>Uses Awaitility rather than sleeps so the suite is stable under load.
 */
class PgmqMessageListenerContainerIntegrationTests {

    private static DataSource dataSource;

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    private static JdbcTemplate jdbc;

    private final ConcurrentLinkedQueue<PgmqMessageListenerContainer<?>> containers =
            new ConcurrentLinkedQueue<>();

    @BeforeAll
    static void setUp() {
        dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("create table if not exists consumer_demo (id bigint primary key, note text)");
    }

    @AfterEach
    void stopContainers() {
        PgmqMessageListenerContainer<?> container;
        while ((container = this.containers.poll()) != null) {
            container.stop();
        }
    }

    private String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        return queue;
    }

    private <T> PgmqMessageListenerContainer<T> start(PgmqMessageListenerContainer<T> container) {
        this.containers.add(container);
        container.start();
        return container;
    }

    @Test
    void consumesAndDeletesOnSuccess() {
        String queue = newQueue("consume_ok");
        CountDownLatch handled = new CountDownLatch(3);
        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().pollDelay(Duration.ofMillis(50)).build())
                .handler((message) -> {
                    received.add(message.payload());
                    handled.countDown();
                })
                .build());

        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2), Map.of("n", 3)));

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.getCount() == 0);
        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(received).hasSize(3);
    }

    @Test
    void archivesOnSuccessWhenConfigured() {
        String queue = newQueue("consume_archive");
        CountDownLatch handled = new CountDownLatch(1);

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.ARCHIVE)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .handler((message) -> handled.countDown())
                .build());

        pgmq.send(queue, Map.of("archive", true));

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.getCount() == 0);
        await().atMost(Duration.ofSeconds(20)).until(
                () -> jdbc.queryForObject("select count(*) from pgmq.a_" + queue, Integer.class) == 1);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();
    }

    @Test
    void redeliversAfterAHandlerCrashWithAnIncrementedReadCount() {
        String queue = newQueue("crash_redeliver");
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();
        CountDownLatch twoDeliveries = new CountDownLatch(2);

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .pollDelay(Duration.ofMillis(50))
                        .maxAttempts(10)
                        .build())
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    twoDeliveries.countDown();
                    throw new IllegalStateException("simulated crash");
                })
                .build());

        pgmq.send(queue, Map.of("crash", true));

        await().atMost(Duration.ofSeconds(30)).until(() -> twoDeliveries.getCount() == 0);

        // PGMQ increments read_ct on every read, which is what poison protection relies on.
        assertThat(readCounts).containsSequence(1, 2);
    }

    @Test
    void deadLettersAPoisonMessageAndStopsDeliveringIt() {
        String queue = newQueue("poison");
        String deadLetter = newQueue("poison_dlq");
        AtomicInteger attempts = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .pollDelay(Duration.ofMillis(50))
                        .maxAttempts(3)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(deadLetter)
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("always fails");
                })
                .build());

        pgmq.send(queue, Map.of("poison", true));

        await().atMost(Duration.ofSeconds(60))
                .until(() -> pgmq.metrics(deadLetter).queueLength() == 1);

        assertThat(pgmq.metrics(queue).queueLength()).as("source queue drained").isZero();
        // The handler runs on deliveries 1, 2 and 3; the third failure exhausts maxAttempts=3 and
        // the message is dead-lettered instead of being redelivered a fourth time.
        assertThat(attempts.get()).isEqualTo(3);

        PgmqMessage<String> dead = pgmq.read(deadLetter, ReadOptions.defaults()).get(0);
        assertThat(dead.headers())
                .containsEntry(DeadLetterHeaders.ORIGINAL_QUEUE, queue)
                .containsEntry(DeadLetterHeaders.READ_COUNT, 3)
                .containsEntry(DeadLetterHeaders.EXCEPTION_TYPE, "java.lang.IllegalStateException")
                .containsEntry(DeadLetterHeaders.EXCEPTION_MESSAGE, "always fails")
                .containsKey(DeadLetterHeaders.FAILED_AT)
                .containsKey(DeadLetterHeaders.ORIGINAL_MESSAGE_ID)
                .hasEntrySatisfying(DeadLetterHeaders.REASON,
                        (reason) -> assertThat(reason.toString()).contains("exhausted maxAttempts=3"));
        assertThat(dead.rawPayload()).contains("poison");
    }

    @Test
    void archivesAPoisonMessageWhenNoDeadLetterQueueIsConfigured() {
        String queue = newQueue("poison_archive");

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .pollDelay(Duration.ofMillis(50))
                        .maxAttempts(2)
                        .failureAction(FailureAction.ARCHIVE)
                        .build())
                .handler((message) -> {
                    throw new IllegalStateException("always fails");
                })
                .build());

        pgmq.send(queue, Map.of("archive_poison", true));

        await().atMost(Duration.ofSeconds(30)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(jdbc.queryForObject("select count(*) from pgmq.a_" + queue, Integer.class)).isEqualTo(1);
    }

    @Test
    void transactionalProcessingCommitsBusinessWriteAndAckTogether() {
        String queue = newQueue("tx_commit_consumer");
        CountDownLatch handled = new CountDownLatch(1);

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .transactional(true)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    jdbc.update("insert into consumer_demo(id, note) values (?, ?) on conflict do nothing",
                            message.id(), "committed");
                    handled.countDown();
                })
                .build());

        pgmq.send(queue, Map.of("tx", "commit"));

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.getCount() == 0);
        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(jdbc.queryForObject(
                "select count(*) from consumer_demo where note = 'committed'", Integer.class)).isPositive();
    }

    @Test
    void transactionalProcessingRollsBackBusinessWriteWhenTheHandlerFails() {
        String queue = newQueue("tx_rollback_consumer");
        AtomicInteger attempts = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .transactional(true)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .pollDelay(Duration.ofMillis(50))
                        .maxAttempts(2)
                        .failureAction(FailureAction.ARCHIVE)
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    jdbc.update("insert into consumer_demo(id, note) values (?, ?) on conflict do nothing",
                            -message.id(), "rolled-back");
                    attempts.incrementAndGet();
                    throw new IllegalStateException("fail after write");
                })
                .build());

        pgmq.send(queue, Map.of("tx", "rollback"));

        await().atMost(Duration.ofSeconds(30)).until(() -> attempts.get() >= 2);

        // The business write must have rolled back together with the (never-reached) ack.
        assertThat(jdbc.queryForObject(
                "select count(*) from consumer_demo where note = 'rolled-back'", Integer.class)).isZero();
    }

    @Test
    void manualAcknowledgementLetsTheHandlerDecide() {
        String queue = newQueue("manual_ack");
        CountDownLatch retried = new CountDownLatch(2);

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.MANUAL)
                        .pollDelay(Duration.ofMillis(50))
                        .visibilityTimeout(Duration.ofSeconds(30))
                        .maxAttempts(10)
                        .build())
                .acknowledgingHandler((message, ack) -> {
                    retried.countDown();
                    if (message.readCount() < 2) {
                        ack.retryLater(Duration.ZERO);
                    }
                    else {
                        ack.acknowledge();
                    }
                })
                .build());

        pgmq.send(queue, Map.of("manual", true));

        await().atMost(Duration.ofSeconds(30)).until(() -> retried.getCount() == 0);
        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
    }

    @Test
    void manualAcknowledgementLeavesTheMessageWhenTheHandlerDoesNothing() {
        String queue = newQueue("manual_noop");
        AtomicInteger deliveries = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.MANUAL)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .pollDelay(Duration.ofMillis(50))
                        .maxAttempts(50)
                        .build())
                .acknowledgingHandler((message, ack) -> deliveries.incrementAndGet())
                .build());

        pgmq.send(queue, Map.of("noop", true));

        // Doing nothing is the safe default: the message keeps coming back.
        await().atMost(Duration.ofSeconds(30)).until(() -> deliveries.get() >= 2);
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);
    }

    @Test
    void batchHandlerReceivesWholeBatches() {
        String queue = newQueue("batch_handler");
        ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
        AtomicInteger total = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().batchSize(10).pollDelay(Duration.ofMillis(50)).build())
                .batchHandler((messages) -> {
                    batchSizes.add(messages.size());
                    total.addAndGet(messages.size());
                })
                .build());

        pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2), Map.of("n", 3), Map.of("n", 4)));

        await().atMost(Duration.ofSeconds(20)).until(() -> total.get() == 4);
        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
        assertThat(batchSizes).isNotEmpty();
    }

    @Test
    void pauseStopsFetchingAndResumeContinues() {
        String queue = newQueue("pause_resume");
        AtomicInteger handled = new AtomicInteger();

        PgmqMessageListenerContainer<String> container =
                start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                        .options(ConsumerOptions.builder().pollDelay(Duration.ofMillis(50)).build())
                        .handler((message) -> handled.incrementAndGet())
                        .build());

        container.pause();
        assertThat(container.isPaused()).isTrue();
        pgmq.send(queue, Map.of("paused", true));

        // Stays put while paused.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(handled.get()).isZero());
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);

        container.resume();
        assertThat(container.isPaused()).isFalse();
        await().atMost(Duration.ofSeconds(20)).until(() -> handled.get() == 1);
    }

    @Test
    void gracefulShutdownDrainsInFlightWorkAndLeavesUnhandledMessages() throws Exception {
        String queue = newQueue("graceful");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        PgmqMessageListenerContainer<String> container =
                PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                        .options(ConsumerOptions.builder()
                                .batchSize(1)
                                .pollDelay(Duration.ofMillis(50))
                                .visibilityTimeout(Duration.ofSeconds(60))
                                .shutdownTimeout(Duration.ofSeconds(20))
                                .build())
                        .handler((message) -> {
                            started.countDown();
                            Thread.sleep(2000);
                            finished.countDown();
                        })
                        .build();
        this.containers.add(container);
        container.start();

        pgmq.send(queue, Map.of("slow", true));
        assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();

        long stopStart = System.nanoTime();
        container.stop();
        Duration stopTook = Duration.ofNanos(System.nanoTime() - stopStart);

        // stop() waited for the in-flight handler instead of abandoning it.
        assertThat(finished.getCount()).isZero();
        assertThat(stopTook).isGreaterThan(Duration.ofMillis(500));
        assertThat(container.isRunning()).isFalse();
        await().atMost(Duration.ofSeconds(10)).until(() -> pgmq.metrics(queue).queueLength() == 0);
    }

    @Test
    void listenerCallbacksReportSuccessAndFailure() {
        String queue = newQueue("listener_hooks");
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch both = new CountDownLatch(2);

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .pollDelay(Duration.ofMillis(50))
                        .visibilityTimeout(Duration.ofSeconds(30))
                        .retryDelay(Duration.ofSeconds(60))
                        .maxAttempts(10)
                        .build())
                .listener(new ConsumerListener() {
                    @Override
                    public void onSuccess(String q, PgmqMessage<?> message, Duration duration) {
                        successes.incrementAndGet();
                        both.countDown();
                    }

                    @Override
                    public void onFailure(String q, PgmqMessage<?> message, Duration duration, Throwable error) {
                        failures.incrementAndGet();
                        both.countDown();
                    }
                })
                .handler((message) -> {
                    if (message.rawPayload().contains("boom")) {
                        throw new IllegalStateException("boom");
                    }
                })
                .build());

        pgmq.send(queue, Map.of("ok", true));
        pgmq.send(queue, Map.of("boom", true));

        await().atMost(Duration.ofSeconds(30)).until(() -> both.getCount() == 0);
        assertThat(successes.get()).isPositive();
        assertThat(failures.get()).isPositive();
    }

    @Test
    void rejectsManualAcknowledgementWithABatchHandler() {
        // A batch handler receives no Acknowledgement, so MANUAL would silently mean "never
        // acknowledge anything" and every batch would be redelivered forever.
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(pgmq, "q", String.class)
                        .options(ConsumerOptions.builder()
                                .acknowledgeMode(AcknowledgeMode.MANUAL)
                                .build())
                        .batchHandler((messages) -> {
                        })
                        .build())
                .withMessageContaining("MANUAL cannot be combined with a batchHandler");
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThat(VirtualThreads.available()).isEqualTo(Runtime.version().feature() >= 21);
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().failureAction(FailureAction.DEAD_LETTER).build())
                .withMessageContaining("deadLetterQueue");
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(pgmq, "q", String.class).build())
                .withMessageContaining("handler");
    }
}
