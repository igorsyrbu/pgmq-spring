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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Publishes queue depth and message age as Micrometer gauges.
 *
 * <p>Registers {@code pgmq.queue.length}, {@code pgmq.queue.visible.length} and
 * {@code pgmq.queue.oldest.message.age} (in seconds) for each configured queue.
 *
 * <p>Gauges are read through a cached snapshot rather than by querying PGMQ on every scrape:
 * {@code pgmq.metrics()} scans the queue table, so letting three gauges each trigger their own
 * query would triple that cost. The snapshot is refreshed at most once per
 * {@link #MIN_REFRESH_INTERVAL_MILLIS}.
 */
public class PgmqQueueGauges implements MeterBinder {

    /** Minimum interval between {@code pgmq.metrics()} calls per queue. */
    public static final long MIN_REFRESH_INTERVAL_MILLIS = 1000;

    private static final Log logger = LogFactory.getLog(PgmqQueueGauges.class);

    private final PgmqOperations pgmq;

    private final List<String> queues;

    private final Map<String, Snapshot> snapshots = new ConcurrentHashMap<>();

    public PgmqQueueGauges(PgmqOperations pgmq, List<String> queues) {
        this.pgmq = pgmq;
        this.queues = List.copyOf(queues);
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
        long now = System.currentTimeMillis();
        long last = current.refreshedAt.get();
        if (now - last >= MIN_REFRESH_INTERVAL_MILLIS && current.refreshedAt.compareAndSet(last, now)) {
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

        private final AtomicLong refreshedAt = new AtomicLong();

        private volatile double queueLength;

        private volatile double visibleLength;

        private volatile double oldestAgeSeconds;
    }
}
