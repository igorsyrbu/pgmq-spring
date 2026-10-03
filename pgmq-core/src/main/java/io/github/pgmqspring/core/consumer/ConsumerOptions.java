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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.github.pgmqspring.core.client.GroupReadStrategy;

/**
 * Configuration for a {@link PgmqMessageListenerContainer}.
 *
 * <p>Built with {@link #builder()}; every value has a defensible default, so the minimum
 * configuration is just a queue name.
 */
public final class ConsumerOptions {

    /** PGMQ takes visibility timeouts as an {@code integer} of seconds; longer delays saturate there. */
    private static final Duration LONGEST_RETRY_DELAY = Duration.ofSeconds(Integer.MAX_VALUE);

    private final int concurrency;

    private final int batchSize;

    private final Duration visibilityTimeout;

    private final Duration pollDelay;

    private final Duration maxPollDelay;

    private final Duration pollJitter;

    private final @Nullable Duration longPoll;

    private final WakeUp wakeUp;

    private final ConsumeMode consumeMode;

    private final boolean groupOrdered;

    private final GroupReadStrategy groupStrategy;

    private final AcknowledgeMode acknowledgeMode;

    private final FailureAction failureAction;

    private final Duration retryDelay;

    private final double retryMultiplier;

    private final @Nullable Duration maxRetryDelay;

    private final int maxAttempts;

    private final List<Class<? extends Throwable>> nonRetryableExceptions;

    private final @Nullable String deadLetterQueue;

    private final boolean transactional;

    private final @Nullable Duration transactionTimeout;

    private final boolean extendLease;

    private final boolean batchAcknowledgements;

    private final @Nullable Integer ackBatchSize;

    private final Duration shutdownTimeout;

    private ConsumerOptions(Builder builder) {
        this.concurrency = builder.concurrency;
        this.batchSize = builder.batchSize;
        this.visibilityTimeout = builder.visibilityTimeout;
        this.pollDelay = builder.pollDelay;
        this.maxPollDelay = builder.maxPollDelay;
        this.pollJitter = builder.pollJitter;
        this.longPoll = builder.longPoll;
        this.wakeUp = builder.wakeUp;
        this.consumeMode = builder.consumeMode;
        this.groupOrdered = builder.groupOrdered;
        this.groupStrategy = builder.groupStrategy;
        this.acknowledgeMode = builder.acknowledgeMode;
        this.failureAction = builder.failureAction;
        this.retryDelay = builder.retryDelay;
        this.retryMultiplier = builder.retryMultiplier;
        this.maxRetryDelay = builder.maxRetryDelay;
        this.maxAttempts = builder.maxAttempts;
        this.nonRetryableExceptions = List.copyOf(builder.nonRetryableExceptions);
        this.deadLetterQueue = builder.deadLetterQueue;
        this.transactional = builder.transactional;
        this.transactionTimeout = builder.transactionTimeout;
        this.extendLease = builder.extendLease;
        this.batchAcknowledgements = builder.batchAcknowledgements;
        this.ackBatchSize = builder.ackBatchSize;
        this.shutdownTimeout = builder.shutdownTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a builder pre-populated with these options, for deriving a per-container variant
     * from shared defaults - typically the {@code ConsumerOptions} bean that Spring Boot binds from
     * {@code pgmq.consumer.*}:
     *
     * <pre>{@code
     * .options(defaults.toBuilder().concurrency(8).deadLetterQueue("orders_dlq").build())
     * }</pre>
     */
    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.concurrency = this.concurrency;
        builder.batchSize = this.batchSize;
        builder.visibilityTimeout = this.visibilityTimeout;
        builder.pollDelay = this.pollDelay;
        builder.maxPollDelay = this.maxPollDelay;
        builder.pollJitter = this.pollJitter;
        builder.longPoll = this.longPoll;
        builder.wakeUp = this.wakeUp;
        builder.consumeMode = this.consumeMode;
        builder.groupOrdered = this.groupOrdered;
        builder.groupStrategy = this.groupStrategy;
        builder.acknowledgeMode = this.acknowledgeMode;
        builder.failureAction = this.failureAction;
        builder.retryDelay = this.retryDelay;
        builder.retryMultiplier = this.retryMultiplier;
        builder.maxRetryDelay = this.maxRetryDelay;
        builder.maxAttempts = this.maxAttempts;
        builder.nonRetryableExceptions = this.nonRetryableExceptions;
        builder.deadLetterQueue = this.deadLetterQueue;
        builder.transactional = this.transactional;
        builder.transactionTimeout = this.transactionTimeout;
        builder.extendLease = this.extendLease;
        builder.batchAcknowledgements = this.batchAcknowledgements;
        builder.ackBatchSize = this.ackBatchSize;
        builder.shutdownTimeout = this.shutdownTimeout;
        return builder;
    }

    /** Default options. */
    public static ConsumerOptions defaults() {
        return builder().build();
    }

    public int getConcurrency() {
        return this.concurrency;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Duration getVisibilityTimeout() {
        return this.visibilityTimeout;
    }

    public Duration getPollDelay() {
        return this.pollDelay;
    }

    public Duration getMaxPollDelay() {
        return this.maxPollDelay;
    }

    public Duration getPollJitter() {
        return this.pollJitter;
    }

    public @Nullable Duration getLongPoll() {
        return this.longPoll;
    }

    public WakeUp getWakeUp() {
        return this.wakeUp;
    }

    public ConsumeMode getConsumeMode() {
        return this.consumeMode;
    }

    /**
     * Whether this container reads with PGMQ's grouped reads, giving at most one in-flight message
     * per group key and in-order delivery within a key.
     *
     * @see io.github.pgmqspring.core.client.FifoGroups
     */
    public boolean isGroupOrdered() {
        return this.groupOrdered;
    }

    /** How a grouped read spreads a batch across groups. Ignored unless group-ordered. */
    public GroupReadStrategy getGroupStrategy() {
        return this.groupStrategy;
    }

    public AcknowledgeMode getAcknowledgeMode() {
        return this.acknowledgeMode;
    }

    public FailureAction getFailureAction() {
        return this.failureAction;
    }

    public Duration getRetryDelay() {
        return this.retryDelay;
    }

    public double getRetryMultiplier() {
        return this.retryMultiplier;
    }

    public @Nullable Duration getMaxRetryDelay() {
        return this.maxRetryDelay;
    }

    /**
     * The backoff before a message that failed on its {@code readCount}-th delivery becomes
     * visible again: {@code retryDelay × retryMultiplier^(readCount - 1)}, capped at
     * {@code maxRetryDelay}. Saturates instead of overflowing.
     */
    public Duration retryDelayAfter(int readCount) {
        Duration ceiling = this.maxRetryDelay != null ? this.maxRetryDelay : LONGEST_RETRY_DELAY;
        if (this.retryDelay.isZero() || this.retryMultiplier == 1.0) {
            return this.retryDelay;
        }
        double millis = this.retryDelay.toMillis() * Math.pow(this.retryMultiplier, Math.max(0, readCount - 1));
        if (!Double.isFinite(millis) || millis >= ceiling.toMillis()) {
            return ceiling;
        }
        return Duration.ofMillis((long) Math.ceil(millis));
    }

    public int getMaxAttempts() {
        return this.maxAttempts;
    }

    public List<Class<? extends Throwable>> getNonRetryableExceptions() {
        return this.nonRetryableExceptions;
    }

    /** Whether {@code error}, or any exception in its cause chain, is one of the non-retryable types. */
    public boolean isNonRetryable(Throwable error) {
        if (this.nonRetryableExceptions.isEmpty()) {
            return false;
        }
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = error; current != null && seen.add(current); current = current.getCause()) {
            for (Class<? extends Throwable> type : this.nonRetryableExceptions) {
                if (type.isInstance(current)) {
                    return true;
                }
            }
        }
        return false;
    }

    public @Nullable String getDeadLetterQueue() {
        return this.deadLetterQueue;
    }

    public boolean isTransactional() {
        return this.transactional;
    }

    public @Nullable Duration getTransactionTimeout() {
        return this.transactionTimeout;
    }

    public boolean isExtendLease() {
        return this.extendLease;
    }

    public boolean isBatchAcknowledgements() {
        return this.batchAcknowledgements;
    }

    /** The configured early-flush threshold, or {@code null} to flush once per polled batch. */
    public @Nullable Integer getAckBatchSize() {
        return this.ackBatchSize;
    }

    public Duration getShutdownTimeout() {
        return this.shutdownTimeout;
    }

    @Override
    public String toString() {
        return "ConsumerOptions[concurrency=" + this.concurrency + ", batchSize=" + this.batchSize
                + ", visibilityTimeout=" + this.visibilityTimeout + ", pollDelay=" + this.pollDelay
                + ", maxPollDelay=" + this.maxPollDelay + ", pollJitter=" + this.pollJitter
                + ", longPoll=" + this.longPoll + ", wakeUp=" + this.wakeUp + ", consumeMode=" + this.consumeMode
                + ", groupOrdered=" + this.groupOrdered + ", groupStrategy=" + this.groupStrategy
                + ", acknowledgeMode=" + this.acknowledgeMode + ", failureAction=" + this.failureAction
                + ", retryDelay=" + this.retryDelay + ", retryMultiplier=" + this.retryMultiplier
                + ", maxRetryDelay=" + this.maxRetryDelay + ", maxAttempts=" + this.maxAttempts
                + ", nonRetryableExceptions=" + this.nonRetryableExceptions.stream().map(Class::getName).toList()
                + ", deadLetterQueue=" + this.deadLetterQueue + ", transactional=" + this.transactional
                + ", transactionTimeout=" + this.transactionTimeout
                + ", extendLease=" + this.extendLease + ", batchAcknowledgements=" + this.batchAcknowledgements
                + ", ackBatchSize=" + this.ackBatchSize + ", shutdownTimeout=" + this.shutdownTimeout + "]";
    }

    /** Builder for {@link ConsumerOptions}. */
    public static final class Builder {

        private int concurrency = 1;

        private int batchSize = 10;

        private Duration visibilityTimeout = Duration.ofSeconds(30);

        private Duration pollDelay = Duration.ofMillis(200);

        private Duration maxPollDelay = Duration.ofSeconds(5);

        private Duration pollJitter = Duration.ZERO;

        private @Nullable Duration longPoll;

        private WakeUp wakeUp = WakeUp.POLL;

        private ConsumeMode consumeMode = ConsumeMode.READ;

        private boolean groupOrdered;

        private GroupReadStrategy groupStrategy = GroupReadStrategy.HEAD;

        private AcknowledgeMode acknowledgeMode = AcknowledgeMode.DELETE;

        private FailureAction failureAction = FailureAction.REDELIVER;

        private Duration retryDelay = Duration.ofSeconds(5);

        private double retryMultiplier = 1.0;

        private @Nullable Duration maxRetryDelay;

        private int maxAttempts = 5;

        private Collection<Class<? extends Throwable>> nonRetryableExceptions = List.of();

        private @Nullable String deadLetterQueue;

        private boolean transactional;

        private @Nullable Duration transactionTimeout;

        private boolean extendLease;

        private boolean batchAcknowledgements;

        private @Nullable Integer ackBatchSize;

        private Duration shutdownTimeout = Duration.ofSeconds(30);

        /** Number of concurrent polling loops. Each one holds a JDBC connection while it reads. */
        public Builder concurrency(int value) {
            this.concurrency = value;
            return this;
        }

        /** Maximum messages fetched per poll. */
        public Builder batchSize(int value) {
            this.batchSize = value;
            return this;
        }

        /**
         * How long a fetched message stays invisible to other consumers.
         *
         * <p>Must comfortably exceed the handler's worst-case duration, or the message will be
         * redelivered while it is still being processed. With a single-message handler and
         * {@code batchSize > 1} the lease starts at the read, so the last message of a batch waits
         * for all the ones before it: the timeout must cover the whole batch. Enable
         * {@link #extendLease(boolean)} when that is hard to predict.
         *
         * <p>PGMQ stores visibility in whole seconds, so a sub-second value is rounded
         * <em>up</em>. Must be positive.
         */
        public Builder visibilityTimeout(Duration value) {
            this.visibilityTimeout = value;
            return this;
        }

        /**
         * Initial delay after an empty poll; doubles on each further empty poll up to
         * {@link #maxPollDelay(Duration)}. Also the re-check interval while paused. Must be positive.
         */
        public Builder pollDelay(Duration value) {
            this.pollDelay = value;
            return this;
        }

        /**
         * Ceiling for the empty-queue backoff, and the pause after a failed poll (for example while
         * the database is unreachable). Must be at least {@link #pollDelay(Duration)}.
         */
        public Builder maxPollDelay(Duration value) {
            this.maxPollDelay = value;
            return this;
        }

        /**
         * Adds a random extra wait, between zero and this, to every sleep after an empty poll and
         * after a failed poll. Instances started together - after a deploy, or when the database
         * comes back - otherwise back off on the same schedule and poll in synchronised bursts.
         * Not applied to empty polls while {@link #longPoll(Duration) long-polling}, where the
         * database does the waiting. Zero, the default, adds nothing. Must not be negative.
         */
        public Builder pollJitter(Duration value) {
            this.pollJitter = value;
            return this;
        }

        /**
         * Waits inside the database for up to this long instead of polling and backing off.
         *
         * <p>This removes empty-poll churn at the cost of holding one JDBC connection per
         * concurrent consumer for the whole window. Size the pool accordingly: at least
         * {@code concurrency} connections for the consumer, plus whatever the application needs.
         *
         * <p>At least one second when set, because PGMQ takes the window in whole seconds.
         */
        public Builder longPoll(@Nullable Duration value) {
            this.longPoll = value;
            return this;
        }

        /**
         * How the polling loops learn of new messages. With {@link WakeUp#NOTIFY} the container
         * holds one extra connection that listens for PGMQ's insert notifications and wakes every
         * loop when one arrives; {@link #maxPollDelay(Duration)} then only bounds a fallback poll,
         * and can be much longer. Retries this container schedules wake it up too, but messages
         * sent with a delay, or made visible again by another consumer, are picked up by the
         * fallback poll. Defaults to {@link WakeUp#POLL}.
         *
         * <p>{@code NOTIFY} requires PGMQ 1.10.0, insert notifications enabled on the queue
         * ({@code PgmqOperations.enableNotifyInsert}), and the PostgreSQL JDBC driver; it cannot be
         * combined with {@link #longPoll(Duration)}.
         */
        public Builder wakeUp(WakeUp value) {
            this.wakeUp = value;
            return this;
        }

        /**
         * How messages are taken off the queue; defaults to {@link ConsumeMode#READ}.
         *
         * <p>{@link ConsumeMode#TRANSACTIONAL_POP} halves the writes per message. It requires
         * {@link #transactional(boolean)}, and works best with a
         * {@link #transactionTimeout(Duration)}, since no lease expires to free a hung handler's
         * message. A failed attempt rolls back, then one statement counts it in {@code read_ct}
         * and applies the retry delay, so attempts survive restarts as in read mode - but a
         * handler that kills the JVM is never counted, and is retried for ever. A batch handler
         * pops a whole batch per transaction; a single-message handler pops one message.
         *
         * <p>{@link ConsumeMode#POP} is at-most-once: a failed message is lost, so it cannot be
         * combined with {@link #transactional(boolean)} or a terminal failure action.
         *
         * <p>Both pop modes require {@link AcknowledgeMode#DELETE}, and cannot be combined with
         * {@link #groupOrdered(boolean)} - {@code pop} ignores FIFO groups - nor with
         * {@link #longPoll(Duration)}, {@link #extendLease(boolean)} or
         * {@link #batchAcknowledgements(boolean)}, which have nothing to act on. An acknowledging
         * handler is rejected for the same reason.
         */
        public Builder consumeMode(ConsumeMode value) {
            this.consumeMode = value;
            return this;
        }

        /**
         * Reads with PGMQ's grouped reads so that, for each
         * {@link io.github.pgmqspring.core.client.FifoGroups#GROUP_HEADER} value, at most one
         * message is in flight at a time and messages are delivered in send order - even with
         * {@code concurrency > 1} or several application instances on the same queue.
         *
         * <p>Producers must send with {@code SendOptions.group(key)} for this to group by anything;
         * messages without the header share one implicit group. Call
         * {@code PgmqOperations.createFifoIndex(queue)} once per queue, or every poll scans the
         * queue table.
         *
         * <p>Requires PGMQ 1.10.0 or later.
         */
        public Builder groupOrdered(boolean value) {
            this.groupOrdered = value;
            return this;
        }

        /**
         * How a grouped read spreads a batch across groups; defaults to
         * {@link GroupReadStrategy#HEAD}.
         *
         * <p>{@code HEAD} is the default rather than PGMQ's own {@code GREEDY} because it is the
         * only strategy where a single batch can never contain two messages from the same group -
         * so per-key ordering holds however the batch is subsequently handled, including by a
         * batch handler that processes its messages out of order or in parallel.
         */
        public Builder groupStrategy(GroupReadStrategy value) {
            this.groupStrategy = value;
            return this;
        }

        /** What to do after a handler returns normally. */
        public Builder acknowledgeMode(AcknowledgeMode value) {
            this.acknowledgeMode = value;
            return this;
        }

        /** What to do after a handler throws. */
        public Builder failureAction(FailureAction value) {
            this.failureAction = value;
            return this;
        }

        /**
         * Backoff applied before a failed message becomes visible again, while it still has
         * attempts left. Zero leaves the message's existing lease in place, so it is redelivered
         * when its visibility timeout expires. Rounded up to whole seconds.
         */
        public Builder retryDelay(Duration value) {
            this.retryDelay = value;
            return this;
        }

        /**
         * Grows {@link #retryDelay(Duration)} with every failed attempt: the delay after the
         * {@code n}-th delivery is {@code retryDelay × retryMultiplier^(n - 1)}, capped at
         * {@link #maxRetryDelay(Duration)}. The attempt number is PGMQ's {@code read_ct}, so the
         * backoff survives restarts and is the same on every instance. The default {@code 1.0}
         * keeps the delay fixed. At least {@code 1.0}.
         */
        public Builder retryMultiplier(double value) {
            this.retryMultiplier = value;
            return this;
        }

        /**
         * Upper bound for a delay grown by {@link #retryMultiplier(double)}. Unset, the default,
         * leaves it uncapped. Must not be shorter than {@link #retryDelay(Duration)}.
         */
        public Builder maxRetryDelay(@Nullable Duration value) {
            this.maxRetryDelay = value;
            return this;
        }

        /**
         * How many delivery attempts a message gets before it is treated as poison.
         *
         * <p>Compared against PGMQ's {@code read_ct}, which the database increments on every read.
         * That makes the count survive consumer restarts without any extra bookkeeping. Once it is
         * exceeded the container stops invoking the handler and applies the terminal action
         * ({@link FailureAction#DEAD_LETTER} or {@link FailureAction#ARCHIVE}) immediately.
         */
        public Builder maxAttempts(int value) {
            this.maxAttempts = value;
            return this;
        }

        /**
         * Exception types that no retry can fix, such as a payload that fails validation. When one
         * is found anywhere in the cause chain of a handler's exception - or of a payload
         * conversion failure - the message skips its remaining attempts and gets the terminal
         * {@link #failureAction(FailureAction)} at once, so a hopeless message neither occupies
         * consumers nor, with {@link #groupOrdered(boolean)}, blocks its group. Replaces any types
         * set before; empty by default.
         */
        @SafeVarargs
        public final Builder nonRetryableExceptions(Class<? extends Throwable>... types) {
            List<Class<? extends Throwable>> list = new ArrayList<>(types.length);
            for (Class<? extends Throwable> type : types) {
                list.add(type);
            }
            return nonRetryableExceptions(list);
        }

        /** As {@link #nonRetryableExceptions(Class[])}, from a collection. */
        public Builder nonRetryableExceptions(Collection<Class<? extends Throwable>> types) {
            this.nonRetryableExceptions = types;
            return this;
        }

        /** Queue that poisoned and failed messages are moved to. */
        public Builder deadLetterQueue(@Nullable String value) {
            this.deadLetterQueue = value;
            return this;
        }

        /**
         * Runs the handler and the acknowledgement in one Spring transaction, so business writes
         * and the ack commit atomically.
         *
         * <p>Requires a {@code PlatformTransactionManager} on the container.
         */
        public Builder transactional(boolean value) {
            this.transactional = value;
            return this;
        }

        /**
         * Bounds each {@link #transactional(boolean) transactional} handler invocation, so a hung
         * handler releases its locks and connection and its message is retried, instead of
         * starving the pool.
         *
         * <p>Spring enforces it at statement boundaries: a statement - the handler's own, or the
         * acknowledgement - that starts after the deadline fails, the transaction rolls back and
         * the message counts as failed; a running statement gets the remaining time as its query
         * timeout. Work between statements, such as a slow remote call or CPU-bound code, is not
         * interrupted. Keep it shorter than {@link #visibilityTimeout(Duration)}, or the message
         * can be redelivered while its transaction is still open. Rounded up to whole seconds,
         * which is what Spring's transaction timeouts take. Unset, the default, means no timeout.
         * Requires {@code transactional}.
         */
        public Builder transactionTimeout(@Nullable Duration value) {
            this.transactionTimeout = value;
            return this;
        }

        /**
         * Periodically extends a message's visibility timeout while its handler is still running.
         *
         * <p>Useful when handler duration varies a lot. The lease covers every message of a
         * polled batch from the moment it is read until it is settled, including messages still
         * waiting their turn behind a slow one, and the messages of a batch handler. It is
         * refreshed every third of the visibility timeout.
         */
        public Builder extendLease(boolean value) {
            this.extendLease = value;
            return this;
        }

        /**
         * Collects the acknowledgements of a polled batch and sends them as one
         * {@code pgmq.delete} (or {@code pgmq.archive}) statement, instead of one statement per
         * message.
         *
         * <p>Applies to single-message handlers, whose messages are otherwise acknowledged one by
         * one; a batch handler is always acknowledged with one statement. Pending acknowledgements
         * are flushed when the polled batch is done, or earlier once {@link #ackBatchSize(Integer)}
         * are pending, and always before the next poll. Until then each message stays leased - and
         * with {@link #extendLease(boolean)} its lease keeps being refreshed - so a crash or a
         * failed flush redelivers it, which at-least-once handlers already tolerate.
         *
         * <p>Cannot be combined with {@link #transactional(boolean)}, where the acknowledgement
         * must commit with the handler's writes, nor with {@link AcknowledgeMode#MANUAL}.
         */
        public Builder batchAcknowledgements(boolean value) {
            this.batchAcknowledgements = value;
            return this;
        }

        /**
         * With {@link #batchAcknowledgements(boolean)}, flushes as soon as this many
         * acknowledgements are pending rather than only at the end of the polled batch. Unset, the
         * default, flushes once per batch. At least 1 when set.
         */
        public Builder ackBatchSize(@Nullable Integer value) {
            this.ackBatchSize = value;
            return this;
        }

        /**
         * How long {@code stop()} waits for in-flight handlers to finish. Handlers still running
         * after this are interrupted, and their messages are redelivered once their lease lapses.
         * In a Spring application keep it below {@code spring.lifecycle.timeout-per-shutdown-phase}.
         */
        public Builder shutdownTimeout(Duration value) {
            this.shutdownTimeout = value;
            return this;
        }

        public ConsumerOptions build() {
            requirePositive(this.visibilityTimeout, "visibilityTimeout");
            // A zero poll delay would turn an empty queue - or a paused container - into a busy
            // loop issuing a query as fast as the database can answer it.
            requirePositive(this.pollDelay, "pollDelay");
            requirePositive(this.maxPollDelay, "maxPollDelay");
            if (this.maxPollDelay.compareTo(this.pollDelay) < 0) {
                throw new IllegalArgumentException("maxPollDelay (" + this.maxPollDelay
                        + ") must not be shorter than pollDelay (" + this.pollDelay + ")");
            }
            requireNonNegative(this.pollJitter, "pollJitter");
            if (this.longPoll != null && this.longPoll.compareTo(Duration.ofSeconds(1)) < 0) {
                // read_with_poll takes whole seconds; anything shorter would silently become 0.
                throw new IllegalArgumentException("longPoll must be at least 1s, or unset, but was " + this.longPoll);
            }
            requireNonNegative(this.retryDelay, "retryDelay");
            if (!(this.retryMultiplier >= 1.0) || Double.isInfinite(this.retryMultiplier)) {
                throw new IllegalArgumentException("retryMultiplier must be a finite number of at least 1.0, but was "
                        + this.retryMultiplier);
            }
            if (this.maxRetryDelay != null && this.maxRetryDelay.compareTo(this.retryDelay) < 0) {
                throw new IllegalArgumentException("maxRetryDelay (" + this.maxRetryDelay
                        + ") must not be shorter than retryDelay (" + this.retryDelay + ")");
            }
            requireNonNegative(this.shutdownTimeout, "shutdownTimeout");
            if (this.groupStrategy == null || this.acknowledgeMode == null || this.failureAction == null
                    || this.wakeUp == null || this.consumeMode == null) {
                throw new IllegalArgumentException(
                        "groupStrategy, acknowledgeMode, failureAction, wakeUp and consumeMode must not be null");
            }
            validateConsumeMode();
            if (this.wakeUp == WakeUp.NOTIFY && this.longPoll != null) {
                throw new IllegalArgumentException("wakeUp=NOTIFY cannot be combined with longPoll: both replace "
                        + "empty-queue polling, and a long poll would hold its connection through every notification");
            }
            if (this.deadLetterQueue != null) {
                io.github.pgmqspring.core.QueueNames.validate(this.deadLetterQueue);
            }
            if (this.concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be at least 1");
            }
            if (this.batchSize < 1) {
                throw new IllegalArgumentException("batchSize must be at least 1");
            }
            if (this.maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be at least 1");
            }
            for (Class<?> type : this.nonRetryableExceptions) {
                // Checked at runtime too: a binder or a raw collection can bypass the generic bound.
                if (type == null || !Throwable.class.isAssignableFrom(type)) {
                    throw new IllegalArgumentException("nonRetryableExceptions must contain only exception types, but "
                            + "contained " + (type != null ? type.getName() : "null"));
                }
            }
            if (this.failureAction == FailureAction.DEAD_LETTER && this.deadLetterQueue == null) {
                throw new IllegalArgumentException(
                        "failureAction=DEAD_LETTER requires deadLetterQueue to be set");
            }
            if (this.ackBatchSize != null && this.ackBatchSize < 1) {
                throw new IllegalArgumentException("ackBatchSize must be at least 1, or unset, but was "
                        + this.ackBatchSize);
            }
            if (this.transactionTimeout != null) {
                requirePositive(this.transactionTimeout, "transactionTimeout");
                if (!this.transactional) {
                    throw new IllegalArgumentException("transactionTimeout requires transactional=true");
                }
            }
            if (this.batchAcknowledgements && this.transactional) {
                throw new IllegalArgumentException("batchAcknowledgements cannot be combined with transactional: "
                        + "a transactional handler's acknowledgement must commit with its own writes");
            }
            if (this.batchAcknowledgements && this.acknowledgeMode == AcknowledgeMode.MANUAL) {
                throw new IllegalArgumentException("batchAcknowledgements cannot be combined with "
                        + "acknowledgeMode=MANUAL, where the handler acknowledges each message itself");
            }
            return new ConsumerOptions(this);
        }

        private void validateConsumeMode() {
            if (this.consumeMode == ConsumeMode.READ) {
                return;
            }
            String mode = "consumeMode=" + this.consumeMode;
            rejectWith(this.groupOrdered, mode + " cannot be combined with groupOrdered: pop ignores FIFO groups");
            rejectWith(this.longPoll != null, mode + " cannot be combined with longPoll: PGMQ has no long-polling pop");
            rejectWith(this.extendLease, mode + " cannot be combined with extendLease: a popped message has no lease");
            rejectWith(this.batchAcknowledgements,
                    mode + " cannot be combined with batchAcknowledgements: pop already removes the message");
            rejectWith(this.acknowledgeMode != AcknowledgeMode.DELETE,
                    mode + " requires acknowledgeMode=DELETE: pop deletes the message");
            if (this.consumeMode == ConsumeMode.TRANSACTIONAL_POP) {
                rejectWith(!this.transactional, mode + " requires transactional=true");
                return;
            }
            rejectWith(this.transactional, mode + " cannot be combined with transactional: pop removes the message "
                    + "before the handler runs; use TRANSACTIONAL_POP to remove it when the handler commits");
            rejectWith(this.failureAction != FailureAction.REDELIVER, mode + " cannot apply failureAction="
                    + this.failureAction + ": a popped message whose handler fails is already gone");
        }

        private static void rejectWith(boolean invalid, String message) {
            if (invalid) {
                throw new IllegalArgumentException(message);
            }
        }

        private static void requirePositive(@Nullable Duration value, String name) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive, but was " + value);
            }
        }

        private static void requireNonNegative(@Nullable Duration value, String name) {
            if (value == null || value.isNegative()) {
                throw new IllegalArgumentException(name + " must not be negative, but was " + value);
            }
        }
    }
}
