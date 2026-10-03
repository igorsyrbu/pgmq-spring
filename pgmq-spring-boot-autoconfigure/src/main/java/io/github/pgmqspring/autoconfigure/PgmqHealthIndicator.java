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

package io.github.pgmqspring.autoconfigure;

import java.util.List;

import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;

import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Reports whether PGMQ is usable.
 *
 * <p>The indicator is DOWN when the {@code pgmq} schema is missing or the database is
 * unreachable, and - if {@code pgmq.health.max-queue-depth} is set - when any monitored queue is
 * deeper than that.
 *
 * <p>Queue depth is only checked for the queues named in {@code pgmq.health.queues}. Checking
 * every queue would make the cost of a health check grow with the number of queues, which is a
 * poor property for something a load balancer polls. Even so, each monitored queue costs one
 * {@code pgmq.metrics()} call per health check, which scans the queue table.
 */
public class PgmqHealthIndicator extends AbstractHealthIndicator {

    private final PgmqOperations pgmq;

    private final PgmqProperties properties;

    public PgmqHealthIndicator(PgmqOperations pgmq, PgmqProperties properties) {
        super("PGMQ health check failed");
        this.pgmq = pgmq;
        this.properties = properties;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        // Capabilities are cached after the first probe, so on their own they would keep reporting
        // UP through a database outage. Listing the queues is a cheap read of PGMQ's own metadata
        // table that actually reaches the database, and fails when PGMQ is not installed.
        int queues;
        try {
            queues = this.pgmq.listQueues().size();
        }
        catch (RuntimeException ex) {
            builder.down(ex).withDetail("error", "PGMQ is unreachable or not installed");
            return;
        }
        PgmqCapabilities capabilities = this.pgmq.capabilities();
        builder.up()
                .withDetail("version", capabilities.version() != null
                        ? capabilities.version().toString() : "unknown (sql-only install)")
                .withDetail("installedAsExtension", capabilities.installedAsExtension())
                .withDetail("queues", queues);

        List<String> monitored = this.properties.getHealth().getQueues();
        if (monitored.isEmpty()) {
            return;
        }
        long threshold = this.properties.getHealth().getMaxQueueDepth();
        for (String queue : monitored) {
            QueueMetrics metrics;
            try {
                metrics = this.pgmq.metrics(queue);
            }
            catch (RuntimeException ex) {
                // A monitored queue that is missing - renamed, dropped, misspelt in configuration -
                // is reported, but does not take the whole indicator DOWN: PGMQ itself is healthy,
                // and a load balancer pulling the instance would not fix the configuration.
                builder.withDetail("queue." + queue + ".error",
                        ex.getMessage() != null ? ex.getMessage() : ex.toString());
                continue;
            }
            builder.withDetail("queue." + queue + ".length", metrics.queueLength());
            builder.withDetail("queue." + queue + ".visibleLength", metrics.visibleLength());
            Integer oldest = metrics.oldestMessageAgeSeconds();
            if (oldest != null) {
                builder.withDetail("queue." + queue + ".oldestMessageAgeSeconds", oldest);
            }
            if (threshold >= 0 && metrics.queueLength() > threshold) {
                builder.down().withDetail("queue." + queue + ".exceededMaxDepth", threshold);
            }
        }
    }
}
