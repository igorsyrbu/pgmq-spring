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

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.micrometer.PgmqMetrics;
import io.github.pgmqspring.core.micrometer.PgmqQueueGauges;

/**
 * Wires Micrometer instrumentation when a {@link MeterRegistry} is available.
 *
 * <p>The unique {@link PgmqMetrics} bean, whether defined here or by the application, is registered
 * on the {@link PgmqTemplate} as its producer-side listener, and attached to every listener
 * container bean as a {@code ConsumerListener}. The configuration keys on {@link PgmqOperations},
 * so an application that supplies its own client still gets the consumer meters and queue gauges.
 */
// afterName rather than after: these classes live in spring-boot-micrometer-metrics, which is an
// optional dependency, so they must be referenced by name to avoid requiring them at runtime.
// The ordering matters: @ConditionalOnBean(MeterRegistry.class) must be evaluated after Micrometer's
// own auto-configuration has registered the registry, or the instrumentation never activates.
@AutoConfiguration(
        after = PgmqAutoConfiguration.class,
        afterName = {
            "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
            "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
            "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple."
                    + "SimpleMetricsExportAutoConfiguration",
        })
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnBean({MeterRegistry.class, PgmqOperations.class})
@ConditionalOnProperty(prefix = "pgmq", name = {"enabled", "metrics.enabled"}, havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(PgmqProperties.class)
public class PgmqMetricsAutoConfiguration {

    private static final Log logger = LogFactory.getLog(PgmqMetricsAutoConfiguration.class);

    /**
     * Attaches {@link PgmqMetrics} to every listener container bean. Static, as a
     * {@code BeanPostProcessor} must be, so that it does not force this configuration class to be
     * instantiated early; the listener itself is resolved lazily, when the first container appears.
     */
    @Bean
    static PgmqListenerContainerMetricsPostProcessor pgmqListenerContainerMetricsPostProcessor(
            ObjectProvider<PgmqMetrics> metrics) {
        return new PgmqListenerContainerMetricsPostProcessor(metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqMetrics pgmqMetrics(MeterRegistry registry) {
        return new PgmqMetrics(registry);
    }

    /**
     * Registers the unique {@link PgmqMetrics} bean as the {@link PgmqTemplate}'s send listener once
     * all singletons exist, so that it applies to an application-defined {@code PgmqMetrics} too.
     */
    @Bean
    public SmartInitializingSingleton pgmqSendMetricsWiring(ObjectProvider<PgmqMetrics> metrics,
            ObjectProvider<PgmqOperations> operations) {
        return () -> {
            PgmqMetrics listener = metrics.getIfUnique();
            PgmqOperations pgmq = operations.getIfUnique();
            if (listener == null || pgmq == null) {
                return;
            }
            if (pgmq instanceof PgmqTemplate template) {
                template.setClientListener(listener);
            }
            else {
                // Send metrics are observed inside PgmqTemplate. A custom PgmqOperations still gets the
                // consumer meters and gauges; it can record sends by delegating to a PgmqTemplate.
                logger.info("The PgmqOperations bean is a " + pgmq.getClass().getName() + ", not a PgmqTemplate, "
                        + "so pgmq.messages.sent and pgmq.send.duration are not recorded for it");
            }
        };
    }

    /**
     * Publishes queue depth and age gauges for the queues named in {@code pgmq.metrics.queues}.
     *
     * <p>Deliberately not guarded by {@code @ConditionalOnProperty}: {@code queues} is a list, so
     * the bound property keys are {@code pgmq.metrics.queues[0]}, {@code [1]} and so on, and a
     * condition on {@code pgmq.metrics.queues} would never match. With an empty list the bean
     * simply registers no meters.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgmqQueueGauges pgmqQueueGauges(PgmqOperations pgmq, PgmqProperties properties) {
        PgmqProperties.Metrics metrics = properties.getMetrics();
        try {
            return new PgmqQueueGauges(pgmq, metrics.getQueues(), metrics.getRefreshInterval(),
                    metrics.getRefreshIntervals());
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalStateException("pgmq.metrics: " + ex.getMessage(), ex);
        }
    }
}
