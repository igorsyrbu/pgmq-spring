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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.micrometer.PgmqMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Consumer metrics under Boot's real Micrometer auto-configuration: a listener container bean
 * built by application code, with no metrics wiring of its own, still reports every consumer
 * meter - and a container that was given the listener explicitly does not report twice.
 */
class ConsumerMetricsAutoConfigurationTests {

    record Job(boolean fail) {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    MetricsAutoConfiguration.class,
                    CompositeMeterRegistryAutoConfiguration.class,
                    SimpleMetricsExportAutoConfiguration.class,
                    PgmqAutoConfiguration.class,
                    PgmqMetricsAutoConfiguration.class))
            .withUserConfiguration(DataSourceConfiguration.class, ContainerConfiguration.class);

    private static final PgmqTemplate pgmq =
            new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());

    private static String newQueues(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        pgmq.createQueue(queue + "_dlq");
        return queue;
    }

    private static double count(MeterRegistry registry, String name, String queue) {
        return registry.find(name).tag("queue", queue).counters().stream().mapToDouble(Counter::count).sum();
    }

    @Test
    void everyConsumerMeterIsRecordedWithoutAnyWiringInTheContainer() {
        String queue = newQueues("consumer_metrics");
        pgmq.send(queue, new Job(false));
        pgmq.send(queue, new Job(true));
        long poisoned = pgmq.send(queue, new Job(false));
        // Already read more often than maxAttempts=1 allows, so it is poison on its next read.
        new JdbcTemplate(PgmqContainerSupport.dataSource())
                .update("update pgmq.q_" + queue + " set read_ct = 5 where msg_id = ?", poisoned);

        this.runner.withPropertyValues("test.queue=" + queue).run((context) -> {
            MeterRegistry registry = context.getBean(MeterRegistry.class);
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> count(registry, "pgmq.messages.dead.lettered", queue) == 2);

            assertThat(count(registry, "pgmq.messages.received", queue)).isEqualTo(3);
            assertThat(count(registry, "pgmq.messages.acknowledged", queue)).isEqualTo(1);
            assertThat(registry.get("pgmq.messages.failed").tag("queue", queue)
                    .tag("exception", "IllegalStateException").counter().count()).isEqualTo(1);
            assertThat(count(registry, "pgmq.messages.poison", queue)).isEqualTo(1);
            assertThat(registry.get("pgmq.messages.dead.lettered").tag("queue", queue)
                    .tag("dead.letter.queue", queue + "_dlq").counter().count()).isEqualTo(2);
            Timer success = registry.get("pgmq.processing.duration").tag("queue", queue)
                    .tag("outcome", "success").timer();
            Timer failure = registry.get("pgmq.processing.duration").tag("queue", queue)
                    .tag("outcome", "failure").timer();
            assertThat(success.count()).isEqualTo(1);
            assertThat(failure.count()).isEqualTo(1);
        });
    }

    @Test
    void aContainerGivenTheListenerExplicitlyIsNotCountedTwice() {
        String queue = newQueues("consumer_metrics_explicit");
        pgmq.send(queue, new Job(false));

        this.runner.withPropertyValues("test.queue=" + queue, "test.explicit-listener=true").run((context) -> {
            MeterRegistry registry = context.getBean(MeterRegistry.class);
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> count(registry, "pgmq.messages.acknowledged", queue) >= 1);
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                    .until(() -> count(registry, "pgmq.messages.acknowledged", queue) == 1
                            && count(registry, "pgmq.messages.received", queue) == 1);
        });
    }

    @Test
    void nothingIsAttachedWhenMetricsAreDisabled() {
        String queue = newQueues("consumer_metrics_off");
        pgmq.send(queue, new Job(false));

        this.runner.withPropertyValues("test.queue=" + queue, "pgmq.metrics.enabled=false").run((context) -> {
            assertThat(context).doesNotHaveBean(PgmqMetrics.class);
            await().atMost(Duration.ofSeconds(30)).until(() -> pgmq.metrics(queue).queueLength() == 0);
            assertThat(context.getBean(MeterRegistry.class).find("pgmq.messages.acknowledged").counters())
                    .isEmpty();
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class DataSourceConfiguration {

        /** The shared pool must outlive each context the runner closes. */
        @Bean(destroyMethod = "")
        DataSource dataSource() {
            return PgmqContainerSupport.dataSource();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ContainerConfiguration {

        @Bean
        PgmqMessageListenerContainer<Job> testContainer(PgmqOperations pgmq, Environment environment,
                org.springframework.beans.factory.ObjectProvider<PgmqMetrics> metrics) {
            String queue = environment.getRequiredProperty("test.queue");
            PgmqMessageListenerContainer.Builder<Job> builder = PgmqMessageListenerContainer
                    .builder(pgmq, queue, Job.class)
                    .options(ConsumerOptions.builder()
                            .pollDelay(Duration.ofMillis(50))
                            .maxPollDelay(Duration.ofMillis(200))
                            .retryDelay(Duration.ZERO)
                            .maxAttempts(1)
                            .failureAction(FailureAction.DEAD_LETTER)
                            .deadLetterQueue(queue + "_dlq")
                            .build())
                    .handler((message) -> {
                        if (message.payload().fail()) {
                            throw new IllegalStateException("job failed");
                        }
                    });
            if (environment.getProperty("test.explicit-listener", Boolean.class, false)) {
                builder.listener(metrics.getObject());
            }
            return builder.build();
        }
    }
}
