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

import javax.sql.DataSource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqNotInstalledException;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.consumer.AcknowledgeMode;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.convert.PayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the auto-configuration: bean creation, back-off, and property binding.
 */
class PgmqAutoConfigurationTests {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    PgmqAutoConfiguration.class,
                    PgmqHealthAutoConfiguration.class,
                    PgmqMetricsAutoConfiguration.class))
            .withUserConfiguration(DataSourceConfiguration.class);

    @Test
    void createsClientAndSupportingBeans() {
        this.runner.run((context) -> {
            assertThat(context).hasSingleBean(PgmqTemplate.class);
            assertThat(context).hasSingleBean(PayloadConverter.class);
            assertThat(context).hasSingleBean(ConsumerOptions.class);
            assertThat(context).hasSingleBean(PgmqInitializer.class);
            assertThat(context.getBean(PayloadConverter.class)).isInstanceOf(JacksonPayloadConverter.class);
        });
    }

    @Test
    void backsOffWhenTheApplicationDefinesItsOwnBeans() {
        this.runner.withUserConfiguration(CustomBeansConfiguration.class).run((context) -> {
            assertThat(context).hasSingleBean(PgmqOperations.class);
            assertThat(context.getBean(PgmqOperations.class))
                    .isSameAs(context.getBean(CustomBeansConfiguration.class).customOperations);
            assertThat(context.getBean(PayloadConverter.class))
                    .isSameAs(context.getBean(CustomBeansConfiguration.class).customConverter);
            // Back-off means the auto-configuration did not register its own bean. The user's
            // bean happens to be a PgmqTemplate too, so assert on the bean name, not the type.
            assertThat(context).doesNotHaveBean("pgmqTemplate");
            assertThat(context).doesNotHaveBean("pgmqPayloadConverter");
        });
    }

    @Test
    void isDisabledByProperty() {
        this.runner.withPropertyValues("pgmq.enabled=false").run((context) -> {
            // Back-off means the auto-configuration did not register its own bean. The user's
            // bean happens to be a PgmqTemplate too, so assert on the bean name, not the type.
            assertThat(context).doesNotHaveBean("pgmqTemplate");
            assertThat(context).doesNotHaveBean("pgmqPayloadConverter");
            assertThat(context).doesNotHaveBean(PgmqInitializer.class);
        });
    }

    @Test
    void backsOffWithoutADataSource() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgmqAutoConfiguration.class))
                .run((context) -> assertThat(context).doesNotHaveBean(PgmqTemplate.class));
    }

    @Test
    void bindsConsumerProperties() {
        this.runner.withPropertyValues(
                "pgmq.consumer.concurrency=4",
                "pgmq.consumer.batch-size=25",
                "pgmq.consumer.visibility-timeout=45s",
                "pgmq.consumer.poll-delay=100ms",
                "pgmq.consumer.long-poll=10s",
                "pgmq.consumer.acknowledge-mode=archive",
                "pgmq.consumer.failure-action=dead-letter",
                "pgmq.consumer.dead-letter-queue=my_dlq",
                "pgmq.consumer.max-attempts=7",
                "pgmq.consumer.transactional=true",
                "pgmq.consumer.extend-lease=true").run((context) -> {
                    ConsumerOptions options = context.getBean(ConsumerOptions.class);
                    assertThat(options.getConcurrency()).isEqualTo(4);
                    assertThat(options.getBatchSize()).isEqualTo(25);
                    assertThat(options.getVisibilityTimeout()).isEqualTo(Duration.ofSeconds(45));
                    assertThat(options.getPollDelay()).isEqualTo(Duration.ofMillis(100));
                    assertThat(options.getLongPoll()).isEqualTo(Duration.ofSeconds(10));
                    assertThat(options.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.ARCHIVE);
                    assertThat(options.getFailureAction()).isEqualTo(FailureAction.DEAD_LETTER);
                    assertThat(options.getDeadLetterQueue()).isEqualTo("my_dlq");
                    assertThat(options.getMaxAttempts()).isEqualTo(7);
                    assertThat(options.isTransactional()).isTrue();
                    assertThat(options.isExtendLease()).isTrue();
                });
    }

    @Test
    void bindsBatchAcknowledgementProperties() {
        this.runner.withPropertyValues(
                "pgmq.consumer.batch-acknowledgements=true",
                "pgmq.consumer.ack-batch-size=50").run((context) -> {
                    ConsumerOptions options = context.getBean(ConsumerOptions.class);
                    assertThat(options.isBatchAcknowledgements()).isTrue();
                    assertThat(options.getAckBatchSize()).isEqualTo(50);
                });
        this.runner.run((context) -> {
            ConsumerOptions options = context.getBean(ConsumerOptions.class);
            assertThat(options.isBatchAcknowledgements()).isFalse();
            assertThat(options.getAckBatchSize()).isNull();
        });
    }

    @Test
    void bindsRetryBackoffProperties() {
        this.runner.withPropertyValues(
                "pgmq.consumer.retry-delay=2s",
                "pgmq.consumer.retry-multiplier=2.5",
                "pgmq.consumer.max-retry-delay=5m").run((context) -> {
                    ConsumerOptions options = context.getBean(ConsumerOptions.class);
                    assertThat(options.getRetryMultiplier()).isEqualTo(2.5);
                    assertThat(options.getMaxRetryDelay()).isEqualTo(Duration.ofMinutes(5));
                    assertThat(options.retryDelayAfter(2)).isEqualTo(Duration.ofSeconds(5));
                });
        this.runner.run((context) -> {
            ConsumerOptions options = context.getBean(ConsumerOptions.class);
            assertThat(options.getRetryMultiplier()).isEqualTo(1.0);
            assertThat(options.getMaxRetryDelay()).isNull();
        });
    }

    @Test
    void rejectsBatchAcknowledgementsWithTransactionalAtStartup() {
        this.runner.withPropertyValues(
                "pgmq.consumer.batch-acknowledgements=true",
                "pgmq.consumer.transactional=true").run((context) -> assertThat(context).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("batchAcknowledgements"));
    }

    @Test
    void createsConfiguredQueuesOnStartup() {
        String queue = PgmqContainerSupport.uniqueQueueName("autoconf");
        String unlogged = PgmqContainerSupport.uniqueQueueName("autoconf_unlogged");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.queues[1].name=" + unlogged,
                "pgmq.queues[1].kind=unlogged").run((context) -> {
                    PgmqTemplate pgmq = context.getBean(PgmqTemplate.class);
                    assertThat(pgmq.queueExists(queue)).isTrue();
                    assertThat(pgmq.listQueues())
                            .filteredOn((info) -> info.name().equals(unlogged))
                            .singleElement()
                            .satisfies((info) -> assertThat(info.unlogged()).isTrue());
                });
    }

    @Test
    void verifiesTheInstallationOnStartup() {
        this.runner.withPropertyValues("pgmq.verify-on-startup=true")
                .run((context) -> assertThat(context).hasNotFailed());
    }

    @Test
    void failsStartupWhenTheInstalledVersionIsTooOld() {
        this.runner.withPropertyValues("pgmq.minimum-version=99.0.0").run((context) -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .rootCause()
                    .isInstanceOf(PgmqNotInstalledException.class)
                    .hasMessageContaining("requires 99.0.0 or later");
        });
    }

    @Test
    void skipsVerificationWhenDisabled() {
        this.runner.withPropertyValues("pgmq.minimum-version=99.0.0", "pgmq.verify-on-startup=false")
                .run((context) -> assertThat(context).hasNotFailed());
    }

    @Test
    void registersHealthIndicatorReportingUp() {
        this.runner.run((context) -> {
            assertThat(context).hasSingleBean(PgmqHealthIndicator.class);
            Health health = context.getBean(PgmqHealthIndicator.class).health();
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsKey("version").containsEntry("installedAsExtension", true);
        });
    }

    @Test
    void healthIndicatorReportsQueueDepthAndHonoursTheThreshold() {
        String queue = PgmqContainerSupport.uniqueQueueName("health_depth");
        this.runner.withPropertyValues(
                "pgmq.queues[0].name=" + queue,
                "pgmq.health.queues[0]=" + queue,
                "pgmq.health.max-queue-depth=1").run((context) -> {
                    PgmqTemplate pgmq = context.getBean(PgmqTemplate.class);
                    PgmqHealthIndicator indicator = context.getBean(PgmqHealthIndicator.class);

                    assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

                    pgmq.send(queue, "one");
                    pgmq.send(queue, "two");

                    Health health = indicator.health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("queue." + queue + ".length", 2L);
                });
    }

    @Test
    void healthIndicatorCanBeDisabled() {
        this.runner.withPropertyValues("pgmq.health.enabled=false")
                .run((context) -> assertThat(context).doesNotHaveBean(PgmqHealthIndicator.class));
    }

    @Test
    void registersMicrometerInstrumentationWhenARegistryIsPresent() {
        this.runner.withUserConfiguration(MeterRegistryConfiguration.class).run((context) -> {
            assertThat(context).hasSingleBean(io.github.pgmqspring.core.micrometer.PgmqMetrics.class);

            String queue = PgmqContainerSupport.uniqueQueueName("metrics");
            PgmqTemplate pgmq = context.getBean(PgmqTemplate.class);
            pgmq.createQueue(queue);
            pgmq.send(queue, "measured");

            MeterRegistry registry = context.getBean(MeterRegistry.class);
            assertThat(registry.get("pgmq.messages.sent").tag("queue", queue).counter().count()).isEqualTo(1);
            assertThat(registry.get("pgmq.send.duration").tag("queue", queue).timer().count()).isEqualTo(1);
        });
    }

    /**
     * Checks the auto-configuration ordering under a realistic wiring.
     *
     * <p>{@link #registersMicrometerInstrumentationWhenARegistryIsPresent()} supplies its own
     * {@code MeterRegistry} from user configuration, which is registered before any
     * auto-configuration runs. That makes {@code @ConditionalOnBean(MeterRegistry.class)} true no
     * matter how our auto-configuration is ordered, so it cannot detect a missing
     * {@code @AutoConfiguration(afterName = ...)}.
     *
     * <p>This test instead lets Boot's own Micrometer auto-configuration create the registry, which
     * is what happens in a real application. Remove the ordering from
     * {@code PgmqMetricsAutoConfiguration} and this test fails while the other one still passes.
     */
    @Test
    void metricsActivateWithBootsOwnMicrometerAutoConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MetricsAutoConfiguration.class,
                        CompositeMeterRegistryAutoConfiguration.class,
                        SimpleMetricsExportAutoConfiguration.class,
                        PgmqAutoConfiguration.class,
                        PgmqMetricsAutoConfiguration.class))
                .withUserConfiguration(DataSourceConfiguration.class)
                .run((context) -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(MeterRegistry.class);
                    assertThat(context)
                            .as("instrumentation must activate when the registry comes from "
                                    + "auto-configuration, not just from a hand-written bean")
                            .hasSingleBean(io.github.pgmqspring.core.micrometer.PgmqMetrics.class);

                    String queue = PgmqContainerSupport.uniqueQueueName("ordered_metrics");
                    PgmqTemplate pgmq = context.getBean(PgmqTemplate.class);
                    pgmq.createQueue(queue);
                    pgmq.send(queue, "measured");

                    assertThat(context.getBean(MeterRegistry.class)
                            .get("pgmq.messages.sent").tag("queue", queue).counter().count()).isEqualTo(1);
                });
    }

    @Test
    void queueGaugesArePublishedForConfiguredQueues() {
        // A list property binds as queues[0], queues[1], so a @ConditionalOnProperty on "queues"
        // would never match; the gauges must register from the list itself.
        String queue = PgmqContainerSupport.uniqueQueueName("gauge");
        // Boot's own Micrometer auto-configuration is what binds MeterBinder beans to the
        // registry, so the gauges only appear under a realistic wiring - a hand-supplied
        // registry would leave them unbound and hide a regression.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MetricsAutoConfiguration.class,
                        CompositeMeterRegistryAutoConfiguration.class,
                        SimpleMetricsExportAutoConfiguration.class,
                        PgmqAutoConfiguration.class,
                        PgmqMetricsAutoConfiguration.class))
                .withUserConfiguration(DataSourceConfiguration.class)
                .withPropertyValues(
                        "pgmq.queues[0].name=" + queue,
                        "pgmq.metrics.queues[0]=" + queue)
                .run((context) -> {
                    assertThat(context).hasSingleBean(
                            io.github.pgmqspring.core.micrometer.PgmqQueueGauges.class);

                    context.getBean(PgmqTemplate.class).send(queue, "measured");
                    MeterRegistry registry = context.getBean(MeterRegistry.class);

                    assertThat(registry.get("pgmq.queue.length").tag("queue", queue).gauge().value())
                            .isEqualTo(1.0);
                    assertThat(registry.find("pgmq.queue.visible.length").tag("queue", queue).gauge())
                            .isNotNull();
                    assertThat(registry.find("pgmq.queue.oldest.message.age").tag("queue", queue).gauge())
                            .isNotNull();
                });
    }

    @Test
    void metricsCanBeDisabled() {
        this.runner.withUserConfiguration(MeterRegistryConfiguration.class)
                .withPropertyValues("pgmq.metrics.enabled=false")
                .run((context) -> assertThat(context)
                        .doesNotHaveBean(io.github.pgmqspring.core.micrometer.PgmqMetrics.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class DataSourceConfiguration {

        /**
         * The container's pool is shared by every test in the JVM, so Spring must not close it
         * when a context shuts down - an ApplicationContextRunner closes its context after each
         * run, which would leave every later test without a working pool.
         */
        @Bean(destroyMethod = "")
        DataSource dataSource() {
            return PgmqContainerSupport.dataSource();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfiguration {

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomBeansConfiguration {

        private final PayloadConverter customConverter = new JacksonPayloadConverter();

        private final PgmqOperations customOperations =
                new PgmqTemplate(PgmqContainerSupport.dataSource(), this.customConverter);

        @Bean
        PayloadConverter payloadConverter() {
            return this.customConverter;
        }

        @Bean
        PgmqOperations pgmqOperations() {
            return this.customOperations;
        }
    }
}
