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

package io.github.pgmqspring.samples.batch;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.consumer.ConsumeMode;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

/**
 * Declares the batch listener container.
 *
 * <p>The only differences from a single-message container are {@code batchHandler(...)} instead
 * of {@code handler(...)}, and a {@code batchSize} worth batching for.
 */
@Configuration(proxyBeanMethods = false)
public class ReadingConsumerConfiguration {

    @Bean
    PgmqMessageListenerContainer<Reading> readingListenerContainer(PgmqOperations pgmq,
            ReadingBatchHandler handler, PlatformTransactionManager transactionManager) {
        return PgmqMessageListenerContainer.builder(pgmq, ReadingService.QUEUE, Reading.class)
                .options(ConsumerOptions.builder()
                        // Up to 50 messages per poll, all handed to the handler in one call.
                        .batchSize(50)
                        .concurrency(2)
                        // Up to 100ms of random extra wait after an empty poll, so the two polling
                        // loops - and other instances started at the same time - drift apart
                        // instead of polling in lockstep.
                        .pollJitter(Duration.ofMillis(100))
                        // The batch is popped inside the handler's transaction: the multi-row insert
                        // and the removal of every message commit together, in one statement per
                        // batch rather than a read and a delete.
                        .consumeMode(ConsumeMode.TRANSACTIONAL_POP)
                        .transactional(true)
                        // No lease expires to free a stuck batch, so the transaction does.
                        .transactionTimeout(Duration.ofSeconds(30))
                        .maxAttempts(3)
                        .retryDelay(Duration.ofSeconds(1))
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue("readings_dlq")
                        .build())
                .transactionManager(transactionManager)
                .batchHandler(handler::handle)
                .build();
    }
}
