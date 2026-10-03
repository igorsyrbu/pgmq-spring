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
        return new PgmqTemplate(new JdbcTemplate(dataSource), payloadConverter);
    }

    /** Defaults for listener containers, taken from {@code pgmq.consumer.*}. */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerOptions pgmqConsumerOptions(PgmqProperties properties) {
        PgmqProperties.Consumer consumer = properties.getConsumer();
        return ConsumerOptions.builder()
                .concurrency(consumer.getConcurrency())
                .batchSize(consumer.getBatchSize())
                .visibilityTimeout(consumer.getVisibilityTimeout())
                .pollDelay(consumer.getPollDelay())
                .maxPollDelay(consumer.getMaxPollDelay())
                .longPoll(consumer.getLongPoll())
                .acknowledgeMode(consumer.getAcknowledgeMode())
                .groupOrdered(consumer.isGroupOrdered())
                .groupStrategy(consumer.getGroupStrategy())
                .failureAction(consumer.getFailureAction())
                .retryDelay(consumer.getRetryDelay())
                .retryMultiplier(consumer.getRetryMultiplier())
                .maxRetryDelay(consumer.getMaxRetryDelay())
                .maxAttempts(consumer.getMaxAttempts())
                .nonRetryableExceptions(consumer.getNonRetryableExceptions())
                .deadLetterQueue(consumer.getDeadLetterQueue())
                .transactional(consumer.isTransactional())
                .extendLease(consumer.isExtendLease())
                .batchAcknowledgements(consumer.isBatchAcknowledgements())
                .ackBatchSize(consumer.getAckBatchSize())
                .shutdownTimeout(consumer.getShutdownTimeout())
                .build();
    }

    /**
     * Keeps {@link PgmqInitializer} eager under {@code spring.main.lazy-initialization=true}.
     * Nothing depends on it - it exists for its side effects - so a lazy context would otherwise
     * never create it, and never verify PGMQ or create the configured queues.
     */
    @Bean
    static LazyInitializationExcludeFilter pgmqInitializerLazyInitializationExcludeFilter() {
        return LazyInitializationExcludeFilter.forBeanTypes(PgmqInitializer.class);
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
        return beanFactory.getBean(DataSource.class);
    }
}
