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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqTemplate;

import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** {@link ConsumerOptions.Builder#retryMultiplier(double)} as the container applies it. */
class RetryBackoffIntegrationTests {

    private static PgmqTemplate pgmq;

    @RegisterExtension
    final ListenerContainers containers = new ListenerContainers();

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
    }

    @Test
    void eachRetryWaitsTwiceAsLongAsTheOneBefore() {
        String queue = newQueue("retry_backoff");
        List<Long> deliveredAtMillis = new CopyOnWriteArrayList<>();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
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
                .build());
        pgmq.send(queue, "doomed");

        await().atMost(Duration.ofSeconds(30)).until(() -> deliveredAtMillis.size() == 4);
        long firstGap = deliveredAtMillis.get(1) - deliveredAtMillis.get(0);
        long secondGap = deliveredAtMillis.get(2) - deliveredAtMillis.get(1);
        long thirdGap = deliveredAtMillis.get(3) - deliveredAtMillis.get(2);
        // Delays of 1s, 2s and 4s; only lower bounds, since a busy machine can only make them longer.
        assertThat(firstGap).isGreaterThanOrEqualTo(900L);
        assertThat(secondGap).isGreaterThanOrEqualTo(1900L);
        assertThat(thirdGap).isGreaterThanOrEqualTo(3900L);
        assertThat(secondGap).isGreaterThan((long) (firstGap * 1.5));
        assertThat(thirdGap).isGreaterThan((long) (secondGap * 1.5));
    }
}
