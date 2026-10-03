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

package io.github.pgmqspring.core.consumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** {@link ConsumerOptions.Builder#retryMultiplier(double)} as the container applies it. */
class RetryBackoffIntegrationTests {

    private static PgmqTemplate pgmq;

    private PgmqMessageListenerContainer<String> container;

    @BeforeAll
    static void setUp() {
        DataSource dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
    }

    @AfterEach
    void stopContainer() {
        if (this.container != null) {
            this.container.stop();
        }
    }

    @Test
    void eachRetryWaitsTwiceAsLongAsTheOneBefore() {
        String queue = PgmqContainerSupport.uniqueQueueName("retry_backoff");
        pgmq.createQueue(queue);
        List<Long> deliveredAtMillis = new CopyOnWriteArrayList<>();

        this.container = PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .retryDelay(Duration.ofSeconds(1))
                        .retryMultiplier(2.0)
                        .maxAttempts(4)
                        .failureAction(FailureAction.ARCHIVE)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> {
                    deliveredAtMillis.add(System.currentTimeMillis());
                    throw new IllegalStateException("always fails");
                })
                .build();
        this.container.start();
        pgmq.send(queue, "doomed");

        await().atMost(Duration.ofSeconds(30)).until(() -> deliveredAtMillis.size() == 4);
        // Delays of 1s, 2s and 4s; the upper bounds leave room for polling and a busy machine.
        assertThat(deliveredAtMillis.get(1) - deliveredAtMillis.get(0)).isBetween(900L, 2500L);
        assertThat(deliveredAtMillis.get(2) - deliveredAtMillis.get(1)).isBetween(1900L, 3500L);
        assertThat(deliveredAtMillis.get(3) - deliveredAtMillis.get(2)).isBetween(3900L, 5500L);
    }
}
