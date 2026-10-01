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

package io.github.pgmqspring.core.micrometer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every meter {@link PgmqMetrics} publishes, with its name and tags, as documented in
 * {@code docs/CONFIGURATION.md}.
 */
class PgmqMetricsTests {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final PgmqMetrics metrics = new PgmqMetrics(this.registry);

    private static final PgmqMessage<String> MESSAGE = new PgmqMessage<>(
            1, 1, OffsetDateTime.now(), null, OffsetDateTime.now(), null, "{}", "{}", "orders");

    @Test
    void publishesEveryDocumentedMeter() {
        this.metrics.onSent("orders", List.of(1L, 2L), Duration.ofMillis(3));
        this.metrics.onPolled("orders", List.of(MESSAGE, MESSAGE));
        this.metrics.onPolled("orders", List.of());
        this.metrics.onSuccess("orders", MESSAGE, Duration.ofMillis(5));
        this.metrics.onFailure("orders", MESSAGE, Duration.ofMillis(7), new IllegalStateException());
        this.metrics.onPoison("orders", MESSAGE);
        this.metrics.onDeadLettered("orders", MESSAGE, "orders_dlq", "reason");
        this.metrics.onPollError("orders", new IllegalArgumentException());

        assertThat(this.registry.get("pgmq.messages.sent").tag("queue", "orders").counter().count()).isEqualTo(2);
        assertThat(this.registry.get("pgmq.send.duration").tag("queue", "orders").timer().count()).isEqualTo(1);
        assertThat(this.registry.get("pgmq.messages.received").tag("queue", "orders").counter().count())
                .isEqualTo(2);
        assertThat(this.registry.get("pgmq.messages.acknowledged").tag("queue", "orders").counter().count())
                .isEqualTo(1);
        assertThat(this.registry.get("pgmq.messages.failed").tag("queue", "orders")
                .tag("exception", "IllegalStateException").counter().count()).isEqualTo(1);
        assertThat(this.registry.get("pgmq.processing.duration").tag("outcome", "success").timer().count())
                .isEqualTo(1);
        assertThat(this.registry.get("pgmq.processing.duration").tag("outcome", "failure").timer().count())
                .isEqualTo(1);
        assertThat(this.registry.get("pgmq.messages.poison").tag("queue", "orders").counter().count())
                .isEqualTo(1);
        assertThat(this.registry.get("pgmq.messages.dead.lettered").tag("dead.letter.queue", "orders_dlq")
                .counter().count()).isEqualTo(1);
        assertThat(this.registry.get("pgmq.poll.errors").tag("exception", "IllegalArgumentException")
                .counter().count()).isEqualTo(1);
    }

    @Test
    void reusesMetersInsteadOfRegisteringThemPerEvent() {
        this.metrics.onSuccess("orders", MESSAGE, Duration.ofMillis(1));
        int meters = this.registry.getMeters().size();
        for (int i = 0; i < 100; i++) {
            this.metrics.onSuccess("orders", MESSAGE, Duration.ofMillis(1));
        }
        assertThat(this.registry.getMeters()).hasSize(meters);
        assertThat(this.registry.get("pgmq.messages.acknowledged").counter().count()).isEqualTo(101);
    }
}
