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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqMessage;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompositeConsumerListener}. */
class CompositeConsumerListenerTests {

    private static final PgmqMessage<String> MESSAGE = new PgmqMessage<>(
            1L, 1, OffsetDateTime.now(), null, OffsetDateTime.now(), null, "{}", "{}", "q");

    @Test
    void noDelegatesGivesANoOpListener() {
        ConsumerListener listener = CompositeConsumerListener.of(List.of());

        // Must not throw.
        listener.onSuccess("q", MESSAGE, Duration.ZERO);
        assertThat(listener).isNotInstanceOf(CompositeConsumerListener.class);
    }

    @Test
    void oneDelegateIsReturnedDirectly() {
        ConsumerListener only = new ConsumerListener() {
        };

        assertThat(CompositeConsumerListener.of(List.of(only))).isSameAs(only);
    }

    @Test
    void everyDelegateReceivesEveryCallback() {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        ConsumerListener listener = CompositeConsumerListener.of(List.of(
                counting(first), counting(second)));

        listener.onPolled("q", List.of(MESSAGE));
        listener.onSuccess("q", MESSAGE, Duration.ZERO);
        listener.onFailure("q", MESSAGE, Duration.ZERO, new IllegalStateException());
        listener.onPoison("q", MESSAGE);
        listener.onDeadLettered("q", MESSAGE, "dlq", "reason");
        listener.onPollError("q", new IllegalStateException());

        assertThat(first).hasValue(6);
        assertThat(second).hasValue(6);
    }

    @Test
    void aThrowingDelegateDoesNotStopTheOthers() {
        AtomicInteger reached = new AtomicInteger();
        ConsumerListener exploding = new ConsumerListener() {
            @Override
            public void onSuccess(String queue, PgmqMessage<?> message, Duration duration) {
                throw new IllegalStateException("listener is broken");
            }
        };

        ConsumerListener listener = CompositeConsumerListener.of(List.of(exploding, counting(reached)));
        listener.onSuccess("q", MESSAGE, Duration.ZERO);

        assertThat(reached).hasValue(1);
    }

    private static ConsumerListener counting(AtomicInteger counter) {
        return new ConsumerListener() {
            @Override
            public void onPolled(String queue, List<? extends PgmqMessage<?>> messages) {
                counter.incrementAndGet();
            }

            @Override
            public void onSuccess(String queue, PgmqMessage<?> message, Duration duration) {
                counter.incrementAndGet();
            }

            @Override
            public void onFailure(String queue, PgmqMessage<?> message, Duration duration, Throwable error) {
                counter.incrementAndGet();
            }

            @Override
            public void onPoison(String queue, PgmqMessage<?> message) {
                counter.incrementAndGet();
            }

            @Override
            public void onDeadLettered(String queue, PgmqMessage<?> msg, String dlq, String reason) {
                counter.incrementAndGet();
            }

            @Override
            public void onPollError(String queue, Throwable error) {
                counter.incrementAndGet();
            }
        };
    }
}
