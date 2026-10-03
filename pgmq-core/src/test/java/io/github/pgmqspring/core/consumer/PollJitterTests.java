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
import java.util.SplittableRandom;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/** {@link ConsumerOptions.Builder#pollJitter(Duration)}. */
class PollJitterTests {

    @Test
    void everySleepLiesBetweenTheBaseDelayAndTheBasePlusTheJitter() {
        Duration base = Duration.ofMillis(200);
        Duration jitter = Duration.ofMillis(100);
        SplittableRandom random = new SplittableRandom(42);

        List<Long> sleeps = IntStream.range(0, 1000)
                .mapToObj((i) -> PgmqMessageListenerContainer.jittered(base, jitter, random).toMillis())
                .toList();

        assertThat(sleeps).allSatisfy((millis) -> assertThat(millis).isBetween(200L, 300L));
        // Spread over the whole range, not stuck at one end.
        assertThat(sleeps.stream().mapToLong(Long::longValue).min().orElseThrow()).isLessThan(210L);
        assertThat(sleeps.stream().mapToLong(Long::longValue).max().orElseThrow()).isGreaterThan(290L);
    }

    @Test
    void zeroJitterLeavesTheDelayUnchanged() {
        SplittableRandom random = new SplittableRandom(42);

        assertThat(PgmqMessageListenerContainer.jittered(Duration.ofMillis(200), Duration.ZERO, random))
                .isEqualTo(Duration.ofMillis(200));
        assertThat(ConsumerOptions.defaults().getPollJitter()).isZero();
    }

    @Test
    void rejectsANegativeJitter() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().pollJitter(Duration.ofMillis(-1)).build())
                .withMessageContaining("pollJitter must not be negative");
        ConsumerOptions options = ConsumerOptions.builder().pollJitter(Duration.ofMillis(100)).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("pollJitter=PT0.1S");
    }

    @Test
    void theContainerJittersItsEmptyPolls() {
        PgmqTemplate pgmq = new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());
        String queue = PgmqContainerSupport.uniqueQueueName("poll_jitter");
        pgmq.createQueue(queue);
        List<Long> polledAtMillis = new CopyOnWriteArrayList<>();

        // With poll-delay equal to max-poll-delay every empty-poll sleep is 100ms, plus the jitter.
        PgmqMessageListenerContainer<String> container = PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .pollDelay(Duration.ofMillis(100))
                        .maxPollDelay(Duration.ofMillis(100))
                        .pollJitter(Duration.ofMillis(400))
                        .build())
                .listener(new ConsumerListener() {
                    @Override
                    public void onPolled(String polledQueue, List<? extends PgmqMessage<?>> batch) {
                        polledAtMillis.add(System.currentTimeMillis());
                    }
                })
                .handler((message) -> { })
                .build();
        container.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> polledAtMillis.size() >= 16);
        }
        finally {
            container.stop();
        }

        List<Long> gaps = IntStream.range(1, 16).mapToObj((i) -> polledAtMillis.get(i) - polledAtMillis.get(i - 1))
                .toList();
        assertThat(gaps).allSatisfy((gap) -> assertThat(gap).isGreaterThanOrEqualTo(100L));
        // Fifteen uniform draws from 0-400ms all landing under 150ms has a probability below 1e-6.
        assertThat(gaps.stream().mapToLong(Long::longValue).max().orElseThrow()).isGreaterThan(250L);
    }
}
