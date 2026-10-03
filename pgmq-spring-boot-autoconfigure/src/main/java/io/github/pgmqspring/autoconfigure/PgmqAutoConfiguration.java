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

import javax.sql.DataSource;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.convert.PayloadConverter;

/**
 * Auto-configuration for the PGMQ client.
 *
 * <p>Every bean is declared {@link ConditionalOnMissingBean}, so defining your own
 * {@code PgmqOperations}, {@code PayloadConverter} or {@code ConsumerOptions} bean replaces the
 * one here rather than conflicting with it.
 */
@AutoConfiguration(after = {DataSourceAutoConfiguration.class, TransactionAutoConfiguration.class})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "pgmq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PgmqProperties.class)
public class PgmqAutoConfiguration {

    /**
     * Creates the containers declared under {@code pgmq.consumers}. Static, as a
     * {@code BeanDefinitionRegistryPostProcessor} must be, so it does not instantiate this class early.
     */
    @Bean
    static PgmqDeclaredConsumersRegistrar pgmqDeclaredConsumersRegistrar() {
        return new PgmqDeclaredConsumersRegistrar();
    }

    /**
     * Keeps {@link PgmqInitializer} eager under {@code spring.main.lazy-initialization=true}.
     * Nothing depends on it - it exists for its side effects - so a lazy context would otherwise
     * never create it, and never verify PGMQ or create the configured queues. Listener containers
     * need no such exclusion: Spring creates every {@code SmartLifecycle} bean to start it, lazy or not.
     */
    @Bean
    static LazyInitializationExcludeFilter pgmqInitializerLazyInitializationExcludeFilter() {
        return LazyInitializationExcludeFilter.forBeanTypes(PgmqInitializer.class);
    }

    /**
     * The payload converter, defaulting to whichever Jackson generation is on the classpath and
     * reusing the application's Spring-managed mapper when there is one.
     */
    @Bean
    @ConditionalOnMissingBean
    public PayloadConverter pgmqPayloadConverter(BeanFactory beanFactory) {
        return PayloadConverterFactory.create(beanFactory);
    }

    /**
     * The PGMQ client.
     *
     * <p>Bound to {@code pgmq.datasource} when set, and to the primary {@link DataSource}
     * otherwise, so an application can keep its queues in a different database from its
     * entities - at the cost of losing the transactional-outbox guarantee between the two.
     */
    @Bean
    @ConditionalOnMissingBean(PgmqOperations.class)
    public PgmqTemplate pgmqTemplate(PgmqProperties properties, PayloadConverter payloadConverter,
            BeanFactory beanFactory) {
        DataSource dataSource = resolveDataSource(properties, beanFactory);
        PgmqTemplate template = new PgmqTemplate(new JdbcTemplate(dataSource), payloadConverter);
        try {
            template.setMaxBatchSize(properties.getProducer().getMaxBatchSize());
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalStateException("pgmq.producer.max-batch-size: " + ex.getMessage(), ex);
        }
        template.setDefaultHeaders(properties.getProducer().getDefaultHeaders());
        return template;
    }

    /** Defaults for listener containers, taken from {@code pgmq.consumer.*}. */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerOptions pgmqConsumerOptions(PgmqProperties properties) {
        try {
            return properties.getConsumer().toOptions();
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalStateException("pgmq.consumer: " + ex.getMessage(), ex);
        }
    }

    /** Verifies the installation and creates configured queues before the application serves traffic. */
    @Bean
    @ConditionalOnMissingBean
    public PgmqInitializer pgmqInitializer(PgmqProperties properties, ObjectProvider<PgmqOperations> pgmq,
            BeanFactory beanFactory) {
        return new PgmqInitializer(properties, pgmq, resolveDataSource(properties, beanFactory));
    }

    static DataSource resolveDataSource(PgmqProperties properties, BeanFactory beanFactory) {
        String name = properties.getDatasource();
        if (name != null && !name.isBlank()) {
            return beanFactory.getBean(name, DataSource.class);
        }
        try {
            return beanFactory.getBean(DataSource.class);
        }
        catch (NoUniqueBeanDefinitionException ex) {
            throw new IllegalStateException("There are several DataSource beans and none is primary; set "
                    + "pgmq.datasource to the name of the one PGMQ should use", ex);
        }
    }
}
