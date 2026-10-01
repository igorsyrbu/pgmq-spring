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

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Contributes a PGMQ {@link HealthIndicator} when Boot's health support is on the classpath.
 *
 * <p>Depends on {@code spring-boot-health} rather than the whole actuator: Boot 4 split health out
 * into its own module, so an application can expose a health indicator without pulling in the
 * endpoint infrastructure.
 */
@AutoConfiguration(after = PgmqAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(PgmqOperations.class)
@ConditionalOnEnabledHealthIndicator("pgmq")
@ConditionalOnProperty(prefix = "pgmq.health", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PgmqHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "pgmqHealthIndicator")
    public PgmqHealthIndicator pgmqHealthIndicator(PgmqOperations pgmq, PgmqProperties properties) {
        return new PgmqHealthIndicator(pgmq, properties);
    }
}
