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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqException;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.SendOptions;
import io.github.pgmqspring.core.convert.PayloadConversionException;

import static io.github.pgmqspring.core.PgmqContainerSupport.countRows;
import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What the container must not mistake for a failure of the message - a failed acknowledgement of
 * handled work, a stop - and how long it waits between polls.
 */
class ContainerFailureSemanticsIntegrationTests {

    record Order(String id, int quantity) {
    }

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    @RegisterExtension
    final ListenerContainers containers = new ListenerContainers();

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
        transactionManager = new DataSourceTransactionManager(PgmqContainerSupport.dataSource());
    }

    private static int count(String table) {
        return countRows("pgmq." + table);
    }

    private <T> PgmqMessageListenerContainer<T> start(PgmqMessageListenerContainer<T> container) {
        return this.containers.start(container);
    }

    /** The template, with the first {@code failures} single-message deletes failing. */
    private static PgmqOperations failingFirstDeletes(int failures) {
        AtomicInteger remaining = new AtomicInteger(failures);
        return (PgmqOperations) Proxy.newProxyInstance(ContainerFailureSemanticsIntegrationTests.class.getClassLoader(),
                new Class<?>[] {PgmqOperations.class}, (proxy, method, args) -> {
                    if (method.getName().equals("delete") && args[1] instanceof Long
                            && remaining.getAndDecrement() > 0) {
                        throw new PgmqException("simulated failure of the acknowledgement");
                    }
                    try {
                        return method.invoke(pgmq, args);
                    }
                    catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    @Test
    void anEmptyLongPollIsNotFollowedByASleep() {
        String queue = newQueue("long_poll_no_sleep");
        ConcurrentLinkedQueue<Long> handledAtMillis = new ConcurrentLinkedQueue<>();
        AtomicInteger polls = new AtomicInteger();

        // A long poll of 1s with a 20s backoff: sleeping after an empty long poll would leave the
        // message below waiting for up to 20 seconds.
        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .longPoll(Duration.ofSeconds(1))
                        .pollDelay(Duration.ofSeconds(20))
                        .maxPollDelay(Duration.ofSeconds(20))
                        .build())
                .listener(new ConsumerListener() {
                    @Override
                    public void onPolled(String polledQueue, List<? extends PgmqMessage<?>> messages) {
                        polls.incrementAndGet();
                    }
                })
                .handler((message) -> handledAtMillis.add(System.currentTimeMillis()))
                .build());
        await().atMost(Duration.ofSeconds(10)).until(() -> polls.get() >= 2);

        long sentAt = System.currentTimeMillis();
        pgmq.send(queue, "after an empty long poll");

        await().atMost(Duration.ofSeconds(15)).until(() -> handledAtMillis.size() == 1);
        assertThat(handledAtMillis.peek() - sentAt).isLessThan(5000L);
    }

    @Test
    void aFailedAcknowledgementOfAHandledMessageIsNotAHandlerFailure() {
        String queue = newQueue("failed_ack");
        String dlq = newQueue("failed_ack_dlq");
        ConcurrentLinkedQueue<Integer> handled = new ConcurrentLinkedQueue<>();
        AtomicInteger failures = new AtomicInteger();

        // Two attempts: the redelivery after the failed delete is the second. Treated as a handler
        // failure, the failed delete would be reported and delay the retry.
        start(PgmqMessageListenerContainer.builder(failingFirstDeletes(1), queue, Order.class)
                .options(ConsumerOptions.builder()
                        .maxAttempts(2)
                        .retryDelay(Duration.ofSeconds(30))
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .transactionManager(transactionManager)
                .listener(new ConsumerListener() {
                    @Override
                    public void onFailure(String polledQueue, PgmqMessage<?> message, Duration took, Throwable error) {
                        failures.incrementAndGet();
                    }
                })
                .handler((message) -> handled.add(message.readCount()))
                .build());
        pgmq.send(queue, new Order("a", 1));

        await().atMost(Duration.ofSeconds(20)).until(() -> count("q_" + queue) == 0);
        assertThat(count("q_" + dlq)).isZero();
        assertThat(failures).hasValue(0);
        assertThat(handled).containsExactly(1, 2);
    }

    @Test
    void aHandlerInterruptedByStopIsLeftForRedelivery() throws InterruptedException {
        String queue = newQueue("interrupted_by_stop");
        String dlq = newQueue("interrupted_by_stop_dlq");
        CountDownLatch handling = new CountDownLatch(1);
        AtomicInteger failures = new AtomicInteger();

        PgmqMessageListenerContainer<Order> container = start(PgmqMessageListenerContainer
                .builder(pgmq, queue, Order.class)
                .options(ConsumerOptions.builder()
                        .maxAttempts(1)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .shutdownTimeout(Duration.ofMillis(500))
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .transactionManager(transactionManager)
                .listener(new ConsumerListener() {
                    @Override
                    public void onFailure(String polledQueue, PgmqMessage<?> message, Duration took, Throwable error) {
                        failures.incrementAndGet();
                    }
                })
                .handler((message) -> {
                    handling.countDown();
                    Thread.sleep(30_000);
                })
                .build());
        pgmq.send(queue, new Order("a", 1));
        assertThat(handling.await(10, TimeUnit.SECONDS)).isTrue();

        container.stop();

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> count("q_" + dlq) == 0);
        assertThat(count("q_" + queue)).isEqualTo(1);
        assertThat(failures).hasValue(0);
    }

    @Test
    void anAsynchronousStopDuringADrainWaitsForIt() throws InterruptedException {
        String queue = newQueue("stop_during_drain");
        CountDownLatch handling = new CountDownLatch(1);
        AtomicLong handlerFinishedAt = new AtomicLong();
        AtomicLong callbackRanAt = new AtomicLong();

        PgmqMessageListenerContainer<String> container = start(PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> {
                    handling.countDown();
                    Thread.sleep(2000);
                    handlerFinishedAt.set(System.nanoTime());
                })
                .build());
        pgmq.send(queue, "slow");
        assertThat(handling.await(10, TimeUnit.SECONDS)).isTrue();

        container.stop(() -> { });
        container.stop(() -> callbackRanAt.set(System.nanoTime()));

        await().atMost(Duration.ofSeconds(10)).until(() -> callbackRanAt.get() != 0);
        assertThat(handlerFinishedAt.get()).isNotZero().isLessThanOrEqualTo(callbackRanAt.get());
    }

    @Test
    void anUndeliverablePoppedMessageIsDeadLetteredWithinTheSamePop() {
        String queue = newQueue("txpop_dead_letter_in_tx");
        String dlq = newQueue("txpop_dead_letter_in_tx_dlq");
        ConcurrentLinkedQueue<Integer> pollSizes = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> handled = new ConcurrentLinkedQueue<>();
        pgmq.sendRaw(queue, "\"not an order\"", SendOptions.none());
        pgmq.sendBatch(queue, List.of(Map.of("id", "a", "quantity", 1), Map.of("id", "b", "quantity", 2)));

        // Rolling back first would show up as a second, smaller poll for the two good messages.
        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(ConsumerOptions.builder()
                        .consumeMode(ConsumeMode.TRANSACTIONAL_POP)
                        .transactional(true)
                        .batchSize(10)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue(dlq)
                        .nonRetryableExceptions(PayloadConversionException.class)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .transactionManager(transactionManager)
                .listener(new ConsumerListener() {
                    @Override
                    public void onPolled(String polledQueue, List<? extends PgmqMessage<?>> messages) {
                        if (!messages.isEmpty()) {
                            pollSizes.add(messages.size());
                        }
                    }
                })
                .batchHandler((batch) -> batch.forEach((message) -> handled.add(message.payload().id())))
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 2 && count("q_" + dlq) == 1);
        assertThat(pollSizes).containsExactly(3);
        assertThat(count("q_" + queue)).isZero();
    }
}
