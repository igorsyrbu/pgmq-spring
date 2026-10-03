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

package io.github.pgmqspring.samples.quickstart;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

/**
 * Declares the listener containers: a transactional one for orders, and one for notifications
 * that acknowledges in batches.
 *
 * <p>It is a {@code SmartLifecycle}, so declaring it as a bean is all that is needed — it starts
 * and stops with the application context, and shuts down gracefully by draining in-flight work
 * without acknowledging it.
 */
@Configuration(proxyBeanMethods = false)
public class OrderConsumerConfiguration {

    @Bean
    PgmqMessageListenerContainer<OrderPlaced> orderListenerContainer(PgmqOperations pgmq,
            OrderHandler handler, PlatformTransactionManager transactionManager) {

        return PgmqMessageListenerContainer.builder(pgmq, OrderService.QUEUE, OrderPlaced.class)
                .options(ConsumerOptions.builder()
                        .concurrency(2)
                        // The handler's write and the acknowledgement commit together, so the
                        // confirmation row and the message's removal cannot get out of step.
                        .transactional(true)
                        .maxAttempts(3)
                        .retryDelay(Duration.ofSeconds(1))
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue("orders_dlq")
                        .build())
                .transactionManager(transactionManager)
                .handler(handler::handle)
                .build();
    }

    /**
     * Built from the {@code pgmq.consumer.*} defaults in {@code application.yaml}, which turn on
     * batch acknowledgements; only the queue and handler are set here.
     */
    @Bean
    PgmqMessageListenerContainer<OrderConfirmed> notificationListenerContainer(PgmqOperations pgmq,
            ConsumerOptions defaults, NotificationHandler handler) {

        return PgmqMessageListenerContainer.builder(pgmq, NotificationHandler.QUEUE, OrderConfirmed.class)
                .options(defaults)
                .handler(handler::handle)
                .build();
    }
}
