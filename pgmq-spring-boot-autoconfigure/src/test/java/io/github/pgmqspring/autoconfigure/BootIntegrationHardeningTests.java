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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
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
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.Jackson2PayloadConverter;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.convert.PayloadConverter;
import io.github.pgmqspring.core.micrometer.PgmqMetrics;
import io.github.pgmqspring.core.micrometer.PgmqQueueGauges;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boot integration under configurations real applications use: a custom client, a misconfigured
 * health check, lazy initialization, and an application still on Jackson 2.
 */
class BootIntegrationHardeningTests {

    record Order(String orderId, int itemCount) {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    MetricsAutoConfiguration.class,
                    CompositeMeterRegistryAutoConfiguration.class,
                    SimpleMetricsExportAutoConfiguration.class,
                    PgmqAutoConfiguration.class,
                    PgmqHealthAutoConfiguration.class,
                    PgmqMetricsAutoConfiguration.class))
            .withUserConfiguration(DataSourceConfiguration.class);

    private static final PgmqTemplate pgmq =
            new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());

    @Test
    void aCustomPgmqOperationsKeepsHealthMetricsGaugesAndTheInitializer() {
        String queue = PgmqContainerSupport.uniqueQueueName("custom_client");
        this.runner.withUserConfiguration(CustomClientConfiguration.class)
                .withPropertyValues("pgmq.queues[0].name=" + queue, "pgmq.metrics.queues[0]=" + queue)
                .run((context) -> {
                    assertThat(context).doesNotHaveBean("pgmqTemplate");
                    assertThat(context.getBean(PgmqOperations.class)).isNotInstanceOf(PgmqTemplate.class);
                    assertThat(context).hasSingleBean(PgmqHealthIndicator.class);
                    assertThat(context).hasSingleBean(PgmqMetrics.class);
                    assertThat(context).hasSingleBean(PgmqQueueGauges.class);
                    assertThat(pgmq.queueExists(queue)).as("the initializer created the queue").isTrue();
                    assertThat(context.getBean(PgmqHealthIndicator.class).health().getStatus()).isEqualTo(Status.UP);
                });
    }

    @Test
    void aMissingMonitoredQueueIsReportedWithoutTakingHealthDown() {
        String present = PgmqContainerSupport.uniqueQueueName("health_present");
        pgmq.createQueue(present);
        this.runner.withPropertyValues("pgmq.health.queues[0]=" + present, "pgmq.health.queues[1]=no_such_queue_h")
                .run((context) -> {
                    Health health = context.getBean(PgmqHealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.UP);
                    assertThat(health.getDetails()).containsKey("queue." + present + ".length")
                            .containsKey("queue.no_such_queue_h.error");
                });
    }

    @Test
    void theInitializerStillRunsUnderLazyInitialization() {
        String queue = PgmqContainerSupport.uniqueQueueName("lazy_init");
        this.runner.withInitializer((context) -> context.addBeanFactoryPostProcessor(
                        new LazyInitializationBeanFactoryPostProcessor()))
                .withPropertyValues("pgmq.queues[0].name=" + queue)
                .run((context) -> assertThat(pgmq.queueExists(queue)).isTrue());
    }

    @Test
    void anApplicationsJackson2MapperIsUsedWhenItHasNoJackson3Mapper() {
        this.runner.withUserConfiguration(Jackson2MapperConfiguration.class).run((context) -> {
            PayloadConverter converter = context.getBean(PayloadConverter.class);
            assertThat(converter).isInstanceOf(Jackson2PayloadConverter.class);
            // The application's naming strategy applies, proving it is the application's mapper.
            assertThat(converter.toJson(new Order("A-1", 2))).contains("\"order_id\"").contains("\"item_count\"");
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
    static class CustomClientConfiguration {

        /** A decorator in the way applications add tracing: not a PgmqTemplate. */
        @Bean
        PgmqOperations customOperations() {
            return (PgmqOperations) Proxy.newProxyInstance(PgmqOperations.class.getClassLoader(),
                    new Class<?>[] {PgmqOperations.class}, (proxy, method, args) -> {
                        try {
                            return method.invoke(pgmq, args);
                        }
                        catch (InvocationTargetException ex) {
                            throw ex.getCause();
                        }
                    });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Jackson2MapperConfiguration {

        @Bean
        com.fasterxml.jackson.databind.ObjectMapper jackson2Mapper() {
            return new com.fasterxml.jackson.databind.ObjectMapper()
                    .setPropertyNamingStrategy(com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);
        }
    }
}
