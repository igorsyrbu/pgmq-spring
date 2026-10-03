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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** The retry backoff that {@link ConsumerOptions} computes from a message's read count. */
class RetryBackoffTests {

    @Test
    void theDefaultDelayIsFixed() {
        ConsumerOptions options = ConsumerOptions.builder().retryDelay(Duration.ofSeconds(2)).build();

        assertThat(options.retryDelayAfter(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(options.retryDelayAfter(4)).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void theDelayGrowsWithEveryAttempt() {
        ConsumerOptions options = ConsumerOptions.builder()
                .retryDelay(Duration.ofSeconds(2))
                .retryMultiplier(2.0)
                .build();

        assertThat(options.retryDelayAfter(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(options.retryDelayAfter(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(options.retryDelayAfter(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(ConsumerOptions.builder().retryDelay(Duration.ofMillis(500)).retryMultiplier(1.5).build()
                .retryDelayAfter(2)).isEqualTo(Duration.ofMillis(750));
    }

    @Test
    void theDelayNeverExceedsTheCap() {
        ConsumerOptions options = ConsumerOptions.builder()
                .retryDelay(Duration.ofSeconds(2))
                .retryMultiplier(2.0)
                .maxRetryDelay(Duration.ofSeconds(5))
                .build();

        assertThat(options.retryDelayAfter(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(options.retryDelayAfter(3)).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.retryDelayAfter(1000)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void anUncappedDelaySaturatesInsteadOfOverflowing() {
        ConsumerOptions options = ConsumerOptions.builder()
                .retryDelay(Duration.ofSeconds(1))
                .retryMultiplier(10.0)
                .maxAttempts(10_000)
                .build();

        assertThat(options.retryDelayAfter(10_000)).isEqualTo(Duration.ofSeconds(Integer.MAX_VALUE));
        assertThat(options.retryDelayAfter(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(Integer.MAX_VALUE));
    }

    @Test
    void aZeroDelayStaysZero() {
        ConsumerOptions options = ConsumerOptions.builder().retryDelay(Duration.ZERO).retryMultiplier(3.0).build();

        assertThat(options.retryDelayAfter(5)).isZero();
    }

    @Test
    void rejectsInvalidBackoff() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().retryMultiplier(0.5).build())
                .withMessageContaining("retryMultiplier must be a finite number of at least 1.0");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().retryMultiplier(Double.NaN).build())
                .withMessageContaining("retryMultiplier");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().retryMultiplier(Double.POSITIVE_INFINITY).build())
                .withMessageContaining("retryMultiplier");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder()
                        .retryDelay(Duration.ofSeconds(5)).maxRetryDelay(Duration.ofSeconds(1)).build())
                .withMessageContaining("maxRetryDelay");
        ConsumerOptions options = ConsumerOptions.builder()
                .retryMultiplier(2.0).maxRetryDelay(Duration.ofMinutes(5)).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("retryMultiplier=2.0", "maxRetryDelay=PT5M");
    }
}
