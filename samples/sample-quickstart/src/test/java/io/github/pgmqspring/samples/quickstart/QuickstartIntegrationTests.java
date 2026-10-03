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

package io.github.pgmqspring.samples.quickstart;

import java.time.Duration;

import javax.sql.DataSource;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.consumer.DeadLetterHeaders;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the quickstart sample: the send-and-consume path a new user follows, plus
 * the auto-configured health and metrics they get for free.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {"spring.sql.init.mode=always", "sample.demo.enabled=false"})
class QuickstartIntegrationTests {

    @Autowired
    private OrderService orderService;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private PgmqTemplate pgmq;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private boolean confirmed(String orderId) {
        return this.jdbc.sql("select count(*) from order_confirmations where order_id = ?")
                .param(orderId)
                .query(Integer.class)
                .single() == 1;
    }

    private boolean notified(String orderId) {
        return this.jdbc.sql("select count(*) from order_notifications where order_id = ?")
                .param(orderId)
                .query(Integer.class)
                .single() == 1;
    }

    @Test
    void anOrderIsPublishedAndConsumed() {
        this.orderService.placeOrder("order-1", "alice", 2500);

        await().atMost(Duration.ofSeconds(30)).until(() -> confirmed("order-1"));

        assertThat(this.pgmq.metrics(OrderService.QUEUE).queueLength()).isZero();
    }

    @Test
    void confirmedOrdersAreNotifiedByAConsumerThatAcknowledgesInBatches() {
        PgmqMessageListenerContainer<?> notifications =
                this.applicationContext.getBean("notificationListenerContainer", PgmqMessageListenerContainer.class);
        assertThat(notifications.getOptions().isBatchAcknowledgements()).isTrue();

        for (int i = 0; i < 5; i++) {
            this.orderService.placeOrder("notify-" + i, "customer-" + i, 100);
        }

        await().atMost(Duration.ofSeconds(45)).until(() -> this.jdbc
                .sql("select count(*) from order_notifications where order_id like 'notify-%'")
                .query(Integer.class).single() == 5);
        await().atMost(Duration.ofSeconds(10))
                .until(() -> this.pgmq.metrics(NotificationHandler.QUEUE).queueLength() == 0);
        assertThat(notified("bad-order")).isFalse();
    }

    @Test
    void severalOrdersAreAllConsumed() {
        for (int i = 0; i < 20; i++) {
            this.orderService.placeOrder("bulk-" + i, "customer-" + i, 100L * i);
        }

        await().atMost(Duration.ofSeconds(45)).until(() -> this.jdbc
                .sql("select count(*) from order_confirmations where order_id like 'bulk-%'")
                .query(Integer.class).single() == 20);
    }

    @Test
    void aRollbackLeavesNeitherRowNorMessage() {
        // The rollback happens after send() has run, so this proves the send joined the
        // transaction - not merely that it was never reached.
        new TransactionTemplate(this.transactionManager).executeWithoutResult((status) -> {
            this.orderService.placeOrder("rolled-back", "alice", 100);
            status.setRollbackOnly();
        });

        assertThat(this.jdbc.sql("select count(*) from orders where id = 'rolled-back'")
                .query(Integer.class).single()).isZero();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> this.jdbc
                .sql("select count(*) from pgmq.q_orders where message->>'orderId' = 'rolled-back'")
                .query(Integer.class).single() == 0 && !confirmed("rolled-back"));
    }

    @Test
    void anUnprocessableOrderEndsUpOnTheDeadLetterQueue() {
        // The handler rejects a negative total, so this message can never succeed.
        this.orderService.placeOrder("bad-order", "mallory", -1);

        await().atMost(Duration.ofSeconds(60))
                .until(() -> this.pgmq.metrics("orders_dlq").queueLength() >= 1);

        // Not retried: an invalid order is non-retryable, so it is dead-lettered on its first delivery.
        assertThat(this.pgmq.read("orders_dlq", ReadOptions.batch(5)))
                .anySatisfy((message) -> {
                    assertThat(message.rawPayload()).contains("bad-order");
                    assertThat(message.header(DeadLetterHeaders.READ_COUNT)).isEqualTo(1);
                    // pgmq.producer.default-headers, carried over from the original message.
                    assertThat(message.header("source")).isEqualTo("quickstart-sample");
                });
        assertThat(confirmed("bad-order")).isFalse();
    }

    @Test
    void theListenerContainerIsAutoStartedAndCanBePaused() {
        @SuppressWarnings("unchecked")
        PgmqMessageListenerContainer<OrderPlaced> container =
                this.applicationContext.getBean("orderListenerContainer", PgmqMessageListenerContainer.class);

        assertThat(container.isRunning()).isTrue();
        assertThat(container.isPaused()).isFalse();

        container.pause();
        assertThat(container.isPaused()).isTrue();
        container.resume();
        assertThat(container.isPaused()).isFalse();
    }

    @Test
    void theStarterContributesHealthAndMetricsWithoutAnyExtraCode() {
        HealthIndicator health = (HealthIndicator) this.applicationContext.getBean("pgmqHealthIndicator");
        assertThat(health.health().getStatus()).isEqualTo(Status.UP);
        assertThat(health.health().getDetails()).containsKey("version");

        this.orderService.placeOrder("metered", "alice", 1);
        MeterRegistry registry = this.applicationContext.getBean(MeterRegistry.class);
        assertThat(registry.get("pgmq.messages.sent").tag("queue", OrderService.QUEUE).counter().count())
                .isPositive();
    }

    @Autowired
    private org.springframework.context.ApplicationContext applicationContext;

    @TestConfiguration(proxyBeanMethods = false)
    static class ContainerConfiguration {

        // The pool is shared by every test in the JVM, so the context must not close it.
        @Bean(destroyMethod = "")
        @Primary
        DataSource dataSource() {
            return PgmqContainerSupport.dataSource();
        }
    }
}
