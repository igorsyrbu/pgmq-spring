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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Publishes queue depth and message age as Micrometer gauges.
 *
 * <p>Registers {@code pgmq.queue.length}, {@code pgmq.queue.visible.length} and
 * {@code pgmq.queue.oldest.message.age} (in seconds) for each configured queue.
 *
 * <p>Gauges are read through a cached snapshot rather than by querying PGMQ on every scrape:
 * {@code pgmq.metrics()} scans the queue table, so letting three gauges each trigger their own
 * query would triple that cost. Each queue's snapshot is refreshed at most once per its refresh
 * interval - {@link #DEFAULT_REFRESH_INTERVAL} unless configured - however often it is scraped.
 */
public class PgmqQueueGauges implements MeterBinder {

    /**
     * Default interval between {@code pgmq.metrics()} calls per queue: inside a typical 15-second
     * scrape interval, so dashboards keep their resolution, at a tenth of the queries of one second.
     */
    public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofSeconds(10);

    private static final Log logger = LogFactory.getLog(PgmqQueueGauges.class);

    private static final long NEVER = Long.MIN_VALUE;

    private final PgmqOperations pgmq;

    private final List<String> queues;

    private final long defaultRefreshMillis;

    /** Overrides by normalized queue name. */
    private final Map<String, Long> refreshMillisOverrides;

    private final LongSupplier clockMillis;

    private final Map<String, Snapshot> snapshots = new ConcurrentHashMap<>();

    /** Gauges for {@code queues}, each refreshed at most every {@link #DEFAULT_REFRESH_INTERVAL}. */
    public PgmqQueueGauges(PgmqOperations pgmq, List<String> queues) {
        this(pgmq, queues, DEFAULT_REFRESH_INTERVAL, Map.of());
    }

    /**
     * Gauges for {@code queues}, each refreshed at most every {@code refreshInterval}, or every
     * interval given for it in {@code refreshIntervals}.
     *
     * @throws IllegalArgumentException if an interval is not positive, or an override names a
     *     queue that is not in {@code queues}, where it would silently do nothing
     */
    public PgmqQueueGauges(PgmqOperations pgmq, List<String> queues, Duration refreshInterval,
            Map<String, Duration> refreshIntervals) {
        this(pgmq, queues, refreshInterval, refreshIntervals, System::currentTimeMillis);
    }

    PgmqQueueGauges(PgmqOperations pgmq, List<String> queues, Duration refreshInterval,
            Map<String, Duration> refreshIntervals, LongSupplier clockMillis) {
        this.pgmq = pgmq;
        this.queues = List.copyOf(queues);
        this.defaultRefreshMillis = positiveMillis(refreshInterval, "refreshInterval");
        List<String> listed = this.queues.stream().map(QueueNames::normalize).toList();
        Map<String, Long> overrides = new LinkedHashMap<>();
        refreshIntervals.forEach((queue, interval) -> {
            String normalized = QueueNames.normalize(queue);
            if (!listed.contains(normalized)) {
                throw new IllegalArgumentException("refreshIntervals names queue '" + queue
                        + "', which is not one of the gauged queues " + this.queues);
            }
            overrides.put(normalized, positiveMillis(interval, "refreshIntervals[" + queue + "]"));
        });
        this.refreshMillisOverrides = Map.copyOf(overrides);
        this.clockMillis = clockMillis;
    }

    private static long positiveMillis(Duration interval, String name) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive, but was " + interval);
        }
        return interval.toMillis();
    }

    /** How often {@code queue}'s gauges re-query {@code pgmq.metrics()} at most. */
    public Duration refreshInterval(String queue) {
        return Duration.ofMillis(refreshMillis(queue));
    }

    private long refreshMillis(String queue) {
        return this.refreshMillisOverrides.getOrDefault(QueueNames.normalize(queue), this.defaultRefreshMillis);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        for (String queue : this.queues) {
            Tags tags = Tags.of(PgmqMetrics.TAG_QUEUE, queue);
            Gauge.builder("pgmq.queue.length", queue, (q) -> snapshot(q).queueLength)
                    .description("Messages in the queue, visible or not")
                    .tags(tags)
                    .register(registry);
            Gauge.builder("pgmq.queue.visible.length", queue, (q) -> snapshot(q).visibleLength)
                    .description("Messages currently available to a consumer")
                    .tags(tags)
                    .register(registry);
            Gauge.builder("pgmq.queue.oldest.message.age", queue, (q) -> snapshot(q).oldestAgeSeconds)
                    .description("Age of the oldest message in the queue")
                    .baseUnit("seconds")
                    .tags(tags)
                    .register(registry);
        }
    }

    private Snapshot snapshot(String queue) {
        Snapshot current = this.snapshots.computeIfAbsent(queue, (q) -> new Snapshot());
        long now = this.clockMillis.getAsLong();
        long last = current.refreshedAt.get();
        boolean due = last == NEVER || now - last >= refreshMillis(queue);
        if (due && current.refreshedAt.compareAndSet(last, now)) {
            try {
                QueueMetrics metrics = this.pgmq.metrics(queue);
                current.queueLength = metrics.queueLength();
                current.visibleLength = metrics.visibleLength();
                Integer oldest = metrics.oldestMessageAgeSeconds();
                current.oldestAgeSeconds = oldest != null ? oldest : 0;
            }
            catch (RuntimeException ex) {
                // A missing queue or a transient database error must not break a metrics scrape.
                logger.debug("Could not refresh PGMQ metrics for queue '" + queue + "'", ex);
            }
        }
        return current;
    }

    private static final class Snapshot {

        private final AtomicLong refreshedAt = new AtomicLong(NEVER);

        private volatile double queueLength;

        private volatile double visibleLength;

        private volatile double oldestAgeSeconds;
    }
}
