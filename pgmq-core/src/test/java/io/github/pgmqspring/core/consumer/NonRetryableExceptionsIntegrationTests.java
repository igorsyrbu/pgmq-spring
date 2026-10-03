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
import java.util.concurrent.ConcurrentLinkedQueue;
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
import io.github.pgmqspring.core.client.SendOptions;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.convert.PayloadConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/** {@link ConsumerOptions.Builder#nonRetryableExceptions(Class[])}: hopeless messages skip their retries. */
class NonRetryableExceptionsIntegrationTests {

    record Order(String id, int quantity) {
    }

    static final class InvalidOrderException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        InvalidOrderException(String message) {
            super(message);
        }
    }

    static final class InvalidOrderCheckedException extends Exception {

        private static final long serialVersionUID = 1L;

        InvalidOrderCheckedException(String message) {
            super(message);
        }
    }

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    private static JdbcTemplate jdbc;

    private final ConcurrentLinkedQueue<PgmqMessageListenerContainer<?>> containers = new ConcurrentLinkedQueue<>();

    @BeforeAll
    static void setUp() {
        DataSource dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbc = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void stopContainers() {
        PgmqMessageListenerContainer<?> container;
        while ((container = this.containers.poll()) != null) {
            container.stop();
        }
    }

    private static String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        return queue;
    }

    private static int count(String table) {
        Integer n = jdbc.queryForObject("select count(*) from pgmq." + table, Integer.class);
        return n != null ? n : 0;
    }

    private static ConsumerOptions.Builder deadLettering(String dlq) {
        return ConsumerOptions.builder()
                .maxAttempts(3)
                .retryDelay(Duration.ZERO)
                .visibilityTimeout(Duration.ofSeconds(1))
                .failureAction(FailureAction.DEAD_LETTER)
                .deadLetterQueue(dlq)
                .nonRetryableExceptions(InvalidOrderException.class, InvalidOrderCheckedException.class,
                        PayloadConversionException.class)
                .pollDelay(Duration.ofMillis(50))
                .maxPollDelay(Duration.ofMillis(100));
    }

    private <T> void start(PgmqMessageListenerContainer<T> container) {
        this.containers.add(container);
        container.start();
    }

    private void assertDeadLetteredOnFirstDelivery(String queue, String dlq, AtomicInteger invocations) {
        await().atMost(Duration.ofSeconds(20)).until(() -> count("q_" + queue) == 0 && count("q_" + dlq) == 1);
        PgmqMessage<String> dead = pgmq.read(dlq, ReadOptions.defaults()).get(0);
        assertThat(((Number) dead.header(DeadLetterHeaders.READ_COUNT)).intValue()).isEqualTo(1);
        assertThat((String) dead.header(DeadLetterHeaders.REASON)).startsWith("not retryable after ");
        assertThat(invocations).hasValueLessThanOrEqualTo(1);
    }

    @Test
    void aListedExceptionIsDeadLetteredOnTheFirstFailure() {
        String queue = newQueue("non_retryable");
        String dlq = newQueue("non_retryable_dlq");
        AtomicInteger invocations = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(deadLettering(dlq).build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    invocations.incrementAndGet();
                    throw new InvalidOrderException("quantity must be positive");
                })
                .build());
        pgmq.send(queue, new Order("a", -1));

        assertDeadLetteredOnFirstDelivery(queue, dlq, invocations);
        assertThat(invocations).hasValue(1);
    }

    @Test
    void aListedExceptionIsFoundThroughWrappingExceptions() {
        String queue = newQueue("non_retryable_wrapped");
        String dlq = newQueue("non_retryable_wrapped_dlq");
        AtomicInteger invocations = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(deadLettering(dlq).build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    invocations.incrementAndGet();
                    throw new IllegalStateException("validation failed",
                            new RuntimeException(new InvalidOrderException("quantity must be positive")));
                })
                .build());
        pgmq.send(queue, new Order("a", -1));

        assertDeadLetteredOnFirstDelivery(queue, dlq, invocations);
    }

    @Test
    void aListedCheckedExceptionThrownByTheHandlerMatches() {
        String queue = newQueue("non_retryable_checked");
        String dlq = newQueue("non_retryable_checked_dlq");
        AtomicInteger invocations = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(deadLettering(dlq).build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    invocations.incrementAndGet();
                    throw new InvalidOrderCheckedException("quantity must be positive");
                })
                .build());
        pgmq.send(queue, new Order("a", -1));

        assertDeadLetteredOnFirstDelivery(queue, dlq, invocations);
    }

    @Test
    void anUnconvertiblePayloadIsDeadLetteredOnceConversionFailuresAreListed() {
        String queue = newQueue("non_retryable_unconvertible");
        String dlq = newQueue("non_retryable_unconvertible_dlq");
        AtomicInteger invocations = new AtomicInteger();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(deadLettering(dlq).build())
                .transactionManager(transactionManager)
                .handler((message) -> invocations.incrementAndGet())
                .build());
        pgmq.sendRaw(queue, "\"not an order\"", SendOptions.none());

        assertDeadLetteredOnFirstDelivery(queue, dlq, invocations);
        assertThat(invocations).hasValue(0);
    }

    @Test
    void anUnlistedExceptionIsStillRetried() {
        String queue = newQueue("retryable");
        String dlq = newQueue("retryable_dlq");
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, Order.class)
                .options(deadLettering(dlq).build())
                .transactionManager(transactionManager)
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    throw new IllegalStateException("downstream unavailable");
                })
                .build());
        pgmq.send(queue, new Order("a", 1));

        await().atMost(Duration.ofSeconds(30)).until(() -> count("q_" + dlq) == 1);
        assertThat(readCounts).containsExactly(1, 2, 3);
        PgmqMessage<String> dead = pgmq.read(dlq, ReadOptions.defaults()).get(0);
        assertThat((String) dead.header(DeadLetterHeaders.REASON)).startsWith("exhausted maxAttempts=3");
    }

    @Test
    void matchesAnywhereInTheCauseChainAndSurvivesACycle() {
        ConsumerOptions options = ConsumerOptions.builder().nonRetryableExceptions(InvalidOrderException.class).build();
        RuntimeException outer = new RuntimeException("outer");
        RuntimeException inner = new RuntimeException("inner", outer);
        outer.initCause(inner);

        assertThat(options.isNonRetryable(new InvalidOrderException("x"))).isTrue();
        assertThat(options.isNonRetryable(new RuntimeException(new InvalidOrderException("x")))).isTrue();
        assertThat(options.isNonRetryable(outer)).isFalse();
        assertThat(ConsumerOptions.defaults().isNonRetryable(new InvalidOrderException("x"))).isFalse();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void rejectsTypesThatAreNotExceptions() {
        List raw = List.of(String.class);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().nonRetryableExceptions(raw).build())
                .withMessageContaining("nonRetryableExceptions must contain only exception types")
                .withMessageContaining("java.lang.String");
        ConsumerOptions options = ConsumerOptions.builder().nonRetryableExceptions(InvalidOrderException.class).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("nonRetryableExceptions=[" + InvalidOrderException.class.getName() + "]");
    }
}
