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

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.client.PgmqOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** How often {@link PgmqQueueGauges} re-queries {@code pgmq.metrics()}. */
class PgmqQueueGaugesTests {

    private final Map<String, AtomicInteger> metricsCalls = new ConcurrentHashMap<>();

    private final AtomicLong clockMillis = new AtomicLong(1_000_000);

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final PgmqOperations pgmq = (PgmqOperations) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {PgmqOperations.class}, (proxy, method, args) -> {
                if (!method.getName().equals("metrics")) {
                    throw new UnsupportedOperationException(method.getName());
                }
                String queue = (String) args[0];
                int calls = this.metricsCalls.computeIfAbsent(queue, (q) -> new AtomicInteger()).incrementAndGet();
                return new QueueMetrics(queue, calls, null, null, calls, OffsetDateTime.now(), calls, null);
            });

    private void bind(List<String> queues, Duration interval, Map<String, Duration> overrides) {
        new PgmqQueueGauges(this.pgmq, queues, interval, overrides, this.clockMillis::get).bindTo(this.registry);
    }

    /** Scrapes every gauge of {@code queue} once, as a registry publishing all meters would. */
    private void scrape(String queue) {
        this.registry.get("pgmq.queue.length").tag("queue", queue).gauge().value();
        this.registry.get("pgmq.queue.visible.length").tag("queue", queue).gauge().value();
        this.registry.get("pgmq.queue.oldest.message.age").tag("queue", queue).gauge().value();
    }

    private void advance(Duration duration) {
        this.clockMillis.addAndGet(duration.toMillis());
    }

    private int calls(String queue) {
        AtomicInteger calls = this.metricsCalls.get(queue);
        return calls != null ? calls.get() : 0;
    }

    @Test
    void byDefaultAQueueIsRefreshedAtMostEveryTenSeconds() {
        new PgmqQueueGauges(this.pgmq, List.of("orders"), PgmqQueueGauges.DEFAULT_REFRESH_INTERVAL, Map.of(),
                this.clockMillis::get).bindTo(this.registry);

        scrape("orders");
        advance(Duration.ofSeconds(5));
        scrape("orders");
        assertThat(calls("orders")).isEqualTo(1);

        advance(Duration.ofSeconds(5));
        scrape("orders");
        assertThat(calls("orders")).isEqualTo(2);
        assertThat(this.registry.get("pgmq.queue.length").tag("queue", "orders").gauge().value()).isEqualTo(2);
        assertThat(new PgmqQueueGauges(this.pgmq, List.of("orders")).refreshInterval("orders"))
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void anOverrideAppliesToItsQueueOnly() {
        bind(List.of("orders", "payments"), Duration.ofSeconds(10), Map.of("Payments", Duration.ofSeconds(2)));

        for (int i = 0; i < 5; i++) {
            scrape("orders");
            scrape("payments");
            advance(Duration.ofSeconds(2));
        }

        assertThat(calls("payments")).isEqualTo(5);
        assertThat(calls("orders")).isEqualTo(1);
    }

    @Test
    void rejectsIntervalsThatCannotWork() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> bind(List.of("orders"), Duration.ZERO, Map.of()))
                .withMessageContaining("refreshInterval must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> bind(List.of("orders"), Duration.ofSeconds(10), Map.of("orders", Duration.ZERO)))
                .withMessageContaining("refreshIntervals[orders] must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> bind(List.of("orders"), Duration.ofSeconds(10),
                        Map.of("audit", Duration.ofSeconds(1))))
                .withMessageContaining("refreshIntervals names queue 'audit'");
    }
}
