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

package io.github.pgmqspring.core.client;

import java.time.Duration;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Options for a read operation.
 *
 * <p><strong>Long polling.</strong> Setting {@link #longPoll(Duration)} switches from
 * {@code pgmq.read()} to {@code pgmq.read_with_poll()}, which blocks <em>inside the database</em>
 * until a message arrives or the poll window expires. That removes empty-poll churn, but it also
 * holds a JDBC connection (and a Postgres backend) for the whole window. Size the connection pool
 * for at least one connection per concurrent long-polling consumer, plus whatever the application
 * itself needs, or the application will starve.
 */
public final class ReadOptions {

    private static final Duration DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    private static final ReadOptions DEFAULTS =
            new ReadOptions(DEFAULT_VISIBILITY_TIMEOUT, 1, null, null, null, GroupReadStrategy.GREEDY);

    private final Duration visibilityTimeout;

    private final int batchSize;

    private final @Nullable Duration longPoll;

    private final @Nullable Duration pollInterval;

    private final @Nullable Map<String, Object> conditional;

    private final GroupReadStrategy groupStrategy;

    private ReadOptions(
            Duration visibilityTimeout,
            int batchSize,
            @Nullable Duration longPoll,
            @Nullable Duration pollInterval,
            @Nullable Map<String, Object> conditional,
            GroupReadStrategy groupStrategy) {
        this.visibilityTimeout = visibilityTimeout;
        this.batchSize = batchSize;
        this.longPoll = longPoll;
        this.pollInterval = pollInterval;
        this.conditional = conditional;
        this.groupStrategy = groupStrategy;
    }

    /** Read a single message with a 30-second visibility timeout. */
    public static ReadOptions defaults() {
        return DEFAULTS;
    }

    /** Read up to {@code batchSize} messages. */
    public static ReadOptions batch(int batchSize) {
        return DEFAULTS.batchSize(batchSize);
    }

    /**
     * Returns a copy with the given visibility timeout: how long a read message stays invisible to
     * other consumers before it becomes deliverable again.
     */
    public ReadOptions visibilityTimeout(Duration timeout) {
        Assert.notNull(timeout, "timeout must not be null");
        return new ReadOptions(timeout, this.batchSize, this.longPoll, this.pollInterval, this.conditional,
                this.groupStrategy);
    }

    /** Returns a copy reading up to {@code size} messages per call. */
    public ReadOptions batchSize(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1, but was " + size);
        }
        return new ReadOptions(this.visibilityTimeout, size, this.longPoll, this.pollInterval, this.conditional,
                this.groupStrategy);
    }

    /**
     * Returns a copy that long-polls for up to {@code maxWait}. Mind the connection-pool sizing
     * described on this class.
     */
    public ReadOptions longPoll(Duration maxWait) {
        Assert.notNull(maxWait, "maxWait must not be null");
        return new ReadOptions(this.visibilityTimeout, this.batchSize, maxWait, this.pollInterval, this.conditional,
                this.groupStrategy);
    }

    /** Returns a copy that long-polls for {@code maxWait}, re-checking every {@code interval}. */
    public ReadOptions longPoll(Duration maxWait, Duration interval) {
        Assert.notNull(maxWait, "maxWait must not be null");
        Assert.notNull(interval, "interval must not be null");
        return new ReadOptions(this.visibilityTimeout, this.batchSize, maxWait, interval, this.conditional,
                this.groupStrategy);
    }

    /**
     * Returns a copy that only reads messages whose payload contains the given JSON fragment
     * (PGMQ's {@code conditional} argument, evaluated with the {@code @>} containment operator).
     */
    public ReadOptions conditional(Map<String, Object> filter) {
        Assert.notNull(filter, "filter must not be null");
        return new ReadOptions(this.visibilityTimeout, this.batchSize, this.longPoll, this.pollInterval,
                Map.copyOf(filter), this.groupStrategy);
    }

    /**
     * Returns a copy using the given strategy when passed to
     * {@link PgmqOperations#readGrouped(String, ReadOptions)}. Ignored by a plain read.
     */
    public ReadOptions groupStrategy(GroupReadStrategy strategy) {
        Assert.notNull(strategy, "strategy must not be null");
        return new ReadOptions(this.visibilityTimeout, this.batchSize, this.longPoll, this.pollInterval,
                this.conditional, strategy);
    }

    public Duration getVisibilityTimeout() {
        return this.visibilityTimeout;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public @Nullable Duration getLongPoll() {
        return this.longPoll;
    }

    public @Nullable Duration getPollInterval() {
        return this.pollInterval;
    }

    public @Nullable Map<String, Object> getConditional() {
        return this.conditional;
    }

    /** The strategy a grouped read uses to spread a batch across groups. */
    public GroupReadStrategy getGroupStrategy() {
        return this.groupStrategy;
    }

    /** Whether this read should use {@code pgmq.read_with_poll()}. */
    public boolean isLongPolling() {
        return this.longPoll != null;
    }
}
