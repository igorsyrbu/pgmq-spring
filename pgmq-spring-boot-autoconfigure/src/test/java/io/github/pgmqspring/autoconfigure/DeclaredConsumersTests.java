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

package io.github.pgmqspring.autoconfigure;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.sql.DataSource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.consumer.Acknowledgement;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.PgmqAcknowledgingMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqBatchMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Containers declared under {@code pgmq.consumers}. */
class DeclaredConsumersTests {

    record Order(String id, int quantity) {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PgmqAutoConfiguration.class, PgmqMetricsAutoConfiguration.class))
            .withUserConfiguration(InfrastructureConfiguration.class, HandlerConfiguration.class);

    /** Every message in the cause chain of the context's startup failure. */
    private static String failure(AssertableApplicationContext context) {
        assertThat(context).hasFailed();
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = context.getStartupFailure(); cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private static String queue(String prefix) {
        return PgmqContainerSupport.uniqueQueueName(prefix);
    }

    @Test
    void aDeclaredConsumerConsumesInheritsTheDefaultsAndAppliesItsOverrides() {
        String queue = queue("declared");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumer.max-attempts=7",
                "pgmq.consumer.concurrency=2",
                "pgmq.consumers.orders.queue=" + queue,
                "pgmq.consumers.orders.handler=orderHandler",
                "pgmq.consumers.orders.concurrency=3").run((context) -> {
                    PgmqMessageListenerContainer<?> container =
                            context.getBean("pgmqConsumer-orders", PgmqMessageListenerContainer.class);
                    ConsumerOptions options = container.getOptions();
                    assertThat(options.getConcurrency()).isEqualTo(3);
                    assertThat(options.getMaxAttempts()).isEqualTo(7);
                    assertThat(container.getQueue()).isEqualTo(queue);
                    assertThat(container.isRunning()).isTrue();

                    context.getBean(PgmqTemplate.class).send(queue, new Order("a", 1));

                    OrderHandler handler = context.getBean(OrderHandler.class);
                    await().atMost(Duration.ofSeconds(20)).until(() -> handler.received.size() == 1);
                    assertThat(handler.received.peek()).isEqualTo(new Order("a", 1));
                    // Metrics are attached the same way as to a hand-built container bean.
                    await().atMost(Duration.ofSeconds(5)).until(() -> context.getBean(MeterRegistry.class)
                            .find("pgmq.messages.acknowledged").tag("queue", queue).counter() != null);
                });
    }

    @Test
    void theQueueDefaultsToTheConsumersName() {
        String queue = queue("declared_named");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers." + queue + ".handler=orderHandler").run((context) -> assertThat(
                        context.getBean("pgmqConsumer-" + queue, PgmqMessageListenerContainer.class).getQueue())
                                .isEqualTo(queue));
    }

    @Test
    void batchAndAcknowledgingHandlersAndLambdaBeansAreWiredByType() {
        String batch = queue("declared_batch");
        String acknowledging = queue("declared_ack");
        String lambda = queue("declared_lambda");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + batch,
                "pgmq.queues[1].name=" + acknowledging,
                "pgmq.queues[2].name=" + lambda,
                "pgmq.consumers.batch.queue=" + batch,
                "pgmq.consumers.batch.handler=batchHandler",
                "pgmq.consumers.ack.queue=" + acknowledging,
                "pgmq.consumers.ack.handler=acknowledgingHandler",
                "pgmq.consumers.ack.acknowledge-mode=manual",
                "pgmq.consumers.lambda.queue=" + lambda,
                "pgmq.consumers.lambda.handler=lambdaHandler").run((context) -> {
                    PgmqTemplate pgmq = context.getBean(PgmqTemplate.class);
                    pgmq.sendBatch(batch, List.of(new Order("b1", 1), new Order("b2", 2)));
                    pgmq.send(acknowledging, new Order("k", 1));
                    pgmq.send(lambda, new Order("l", 1));

                    HandlerConfiguration handlers = context.getBean(HandlerConfiguration.class);
                    await().atMost(Duration.ofSeconds(20)).until(() -> handlers.batches.size() == 1
                            && handlers.acknowledged.size() == 1 && handlers.lambdaOrders.size() == 1);
                    assertThat(handlers.batches.peek()).isEqualTo(2);
                    // The lambda's payload type came from its @Bean method's generic return type.
                    assertThat(handlers.lambdaOrders.peek()).isEqualTo(new Order("l", 1));
                    await().atMost(Duration.ofSeconds(10)).until(() -> pgmq.metrics(acknowledging).queueLength() == 0);
                });
    }

    @Test
    void aMismatchedPayloadTypeFailsStartup() {
        String queue = queue("declared_mismatch");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers.orders.queue=" + queue,
                "pgmq.consumers.orders.handler=orderHandler",
                "pgmq.consumers.orders.payload-type=java.lang.Integer").run((context) -> assertThat(failure(context))
                        .contains("pgmq.consumers.orders.payload-type is java.lang.Integer")
                        .contains(Order.class.getName()));
    }

    @Test
    void anUntypedHandlerNeedsAPayloadType() {
        String queue = queue("declared_untyped");
        ApplicationContextRunner untyped = this.runner.withBean("untypedHandler", PgmqMessageHandler.class,
                () -> (PgmqMessageHandler<Object>) (message) -> { });
        untyped.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers.orders.queue=" + queue,
                "pgmq.consumers.orders.handler=untypedHandler").run((context) -> assertThat(failure(context))
                        .contains("pgmq.consumers.orders.payload-type is required"));
        untyped.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers.orders.queue=" + queue,
                "pgmq.consumers.orders.handler=untypedHandler",
                "pgmq.consumers.orders.payload-type=java.lang.String").run((context) -> assertThat(context)
                        .hasNotFailed().hasBean("pgmqConsumer-orders"));
    }

    @Test
    void aMissingOrInvalidHandlerFailsStartup() {
        this.runner.withPropertyValues("pgmq.consumers.orders.handler=noSuchHandler")
                .run((context) -> assertThat(failure(context))
                        .contains("pgmq.consumers.orders.handler names bean 'noSuchHandler'"));
        this.runner.withPropertyValues("pgmq.consumers.orders.queue=orders")
                .run((context) -> assertThat(failure(context)).contains("pgmq.consumers.orders.handler is required"));
        this.runner.withPropertyValues("pgmq.consumers.orders.handler=dataSource", "pgmq.consumers.orders.payload-type="
                + "java.lang.String").run((context) -> assertThat(failure(context))
                        .contains("is not a PgmqMessageHandler"));
    }

    @Test
    void aQueueDeclaredTwiceOrAnInvalidOptionFailsStartup() {
        this.runner.withPropertyValues(
                "pgmq.consumers.first.queue=orders",
                "pgmq.consumers.first.handler=orderHandler",
                "pgmq.consumers.second.queue=Orders",
                "pgmq.consumers.second.handler=orderHandler").run((context) -> assertThat(failure(context))
                        .contains("pgmq.consumers.first and pgmq.consumers.second both consume queue"));
        this.runner.withPropertyValues(
                "pgmq.consumers.orders.handler=orderHandler",
                "pgmq.consumers.orders.failure-action=dead-letter").run((context) -> assertThat(failure(context))
                        .contains("pgmq.consumers.orders: failureAction=DEAD_LETTER requires deadLetterQueue"));
    }

    @Test
    void consumerDefaultsDoNotApplyEntryOnlyKeys() {
        String queue = queue("declared_entry_only");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumer.queue=some_other_queue",
                "pgmq.consumer.handler=noSuchHandler",
                "pgmq.consumer.auto-startup=false",
                "pgmq.consumers." + queue + ".handler=orderHandler").run((context) -> {
                    PgmqMessageListenerContainer<?> container =
                            context.getBean("pgmqConsumer-" + queue, PgmqMessageListenerContainer.class);
                    assertThat(container.getQueue()).isEqualTo(queue);
                    assertThat(container.isRunning()).isTrue();
                });
    }

    @Test
    void aBlankQueueFallsBackToTheConsumersName() {
        String queue = queue("declared_blank_queue");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers." + queue + ".queue=",
                "pgmq.consumers." + queue + ".handler=orderHandler").run((context) -> assertThat(
                        context.getBean("pgmqConsumer-" + queue, PgmqMessageListenerContainer.class).getQueue())
                                .isEqualTo(queue));
    }

    @Test
    void aDeclaredConsumerStartsUnderLazyInitialization() {
        String queue = queue("declared_lazy");
        this.runner.withInitializer((context) -> context
                .addBeanFactoryPostProcessor(new LazyInitializationBeanFactoryPostProcessor()))
                .withPropertyValues(
                        "pgmq.queues[0].name=" + queue,
                        "pgmq.consumers." + queue + ".handler=orderHandler").run((context) -> {
                            context.getBean(PgmqTemplate.class).send(queue, new Order("lazy", 1));
                            OrderHandler handler = context.getBean(OrderHandler.class);
                            await().atMost(Duration.ofSeconds(20)).until(() -> handler.received.size() == 1);
                        });
    }

    @Test
    void aConsumerCanBeLeftStopped() {
        String queue = queue("declared_stopped");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.consumers.orders.queue=" + queue,
                "pgmq.consumers.orders.handler=orderHandler",
                "pgmq.consumers.orders.auto-startup=false",
                "pgmq.consumers.orders.failure-action=archive").run((context) -> {
                    PgmqMessageListenerContainer<?> container =
                            context.getBean("pgmqConsumer-orders", PgmqMessageListenerContainer.class);
                    assertThat(container.isRunning()).isFalse();
                    assertThat(container.getOptions().getFailureAction()).isEqualTo(FailureAction.ARCHIVE);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class InfrastructureConfiguration {

        @Bean(destroyMethod = "")
        DataSource dataSource() {
            return PgmqContainerSupport.dataSource();
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    static class OrderHandler implements PgmqMessageHandler<Order> {

        final ConcurrentLinkedQueue<Order> received = new ConcurrentLinkedQueue<>();

        @Override
        public void handle(PgmqMessage<Order> message) {
            this.received.add(message.payload());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class HandlerConfiguration {

        final ConcurrentLinkedQueue<Integer> batches = new ConcurrentLinkedQueue<>();

        final ConcurrentLinkedQueue<Long> acknowledged = new ConcurrentLinkedQueue<>();

        final ConcurrentLinkedQueue<Order> lambdaOrders = new ConcurrentLinkedQueue<>();

        @Bean
        OrderHandler orderHandler() {
            return new OrderHandler();
        }

        @Bean
        PgmqBatchMessageHandler<Order> batchHandler() {
            return (messages) -> this.batches.add(messages.size());
        }

        @Bean
        PgmqAcknowledgingMessageHandler<Order> acknowledgingHandler() {
            return (PgmqMessage<Order> message, Acknowledgement acknowledgement) -> {
                acknowledgement.acknowledge();
                this.acknowledged.add(message.id());
            };
        }

        @Bean
        PgmqMessageHandler<Order> lambdaHandler() {
            return (message) -> this.lambdaOrders.add(message.payload());
        }
    }
}
