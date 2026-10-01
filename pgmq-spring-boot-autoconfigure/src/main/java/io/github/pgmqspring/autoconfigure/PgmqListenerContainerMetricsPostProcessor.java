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

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;

import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;
import io.github.pgmqspring.core.micrometer.PgmqMetrics;

/**
 * Attaches the Micrometer {@link PgmqMetrics} listener to every
 * {@link PgmqMessageListenerContainer} bean, so consumer metrics need no wiring.
 *
 * <p>Containers are built by application code, which has no reason to know about metrics; without
 * this, every container would have to be handed the listener explicitly, and one that was not would
 * silently report nothing. A container already given the same listener is left as it is.
 */
class PgmqListenerContainerMetricsPostProcessor implements BeanPostProcessor {

    private final ObjectProvider<PgmqMetrics> metrics;

    PgmqListenerContainerMetricsPostProcessor(ObjectProvider<PgmqMetrics> metrics) {
        this.metrics = metrics;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof PgmqMessageListenerContainer<?> container) {
            PgmqMetrics listener = this.metrics.getIfAvailable();
            if (listener != null) {
                container.addListener(listener);
            }
        }
        return bean;
    }
}
