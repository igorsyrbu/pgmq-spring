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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import io.github.pgmqspring.core.QueueKind;
import io.github.pgmqspring.core.client.GroupReadStrategy;
import io.github.pgmqspring.core.consumer.AcknowledgeMode;
import io.github.pgmqspring.core.consumer.ConsumeMode;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.WakeUp;

/**
 * Configuration properties for PGMQ, all under the single {@code pgmq} namespace.
 */
@ConfigurationProperties("pgmq")
public class PgmqProperties {

    /** Whether PGMQ support is enabled. */
    private boolean enabled = true;

    /**
     * Name of the DataSource bean to use. When empty, the application's primary DataSource is
     * used. Set this to run PGMQ against a database other than the application's main one.
     */
    private @Nullable String datasource;

    /**
     * Whether to create the pgmq extension on startup when the database does not have it. Nothing
     * is executed when PGMQ is already installed. Needs the CREATE privilege on the database.
     */
    private boolean createExtension = true;

    /** Whether to check on startup that PGMQ is installed and new enough. */
    private boolean verifyOnStartup = true;

    /**
     * Lowest acceptable PGMQ version. Startup fails when the database has an older one. Defaults
     * to the oldest version this library supports.
     */
    private String minimumVersion = "1.5.0";

    /** Queues to create on startup if they do not already exist. */
    private List<Queue> queues = new ArrayList<>();

    /** Settings for sending. */
    private final Producer producer = new Producer();

    /** Default settings for listener containers. */
    private final Consumer consumer = new Consumer();

    /**
     * Listener containers to create, by name. Each inherits pgmq.consumer.* and overrides what it
     * sets, and becomes a bean named pgmqConsumer-{name}. Entries are read straight from the
     * Environment when the application context starts; this property exists for IDE completion.
     */
    private Map<String, DeclaredConsumer> consumers = new LinkedHashMap<>();

    /** Health indicator settings. */
    private final Health health = new Health();

    /** Metrics settings. */
    private final Metrics metrics = new Metrics();

    public boolean isEnabled() {
        return this.enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public @Nullable String getDatasource() {
        return this.datasource;
    }

    public void setDatasource(@Nullable String datasource) {
        this.datasource = datasource;
    }

    public boolean isCreateExtension() {
        return this.createExtension;
    }

    public void setCreateExtension(boolean createExtension) {
        this.createExtension = createExtension;
    }

    public boolean isVerifyOnStartup() {
        return this.verifyOnStartup;
    }

    public void setVerifyOnStartup(boolean verifyOnStartup) {
        this.verifyOnStartup = verifyOnStartup;
    }

    public String getMinimumVersion() {
        return this.minimumVersion;
    }

    public void setMinimumVersion(String minimumVersion) {
        this.minimumVersion = minimumVersion;
    }

    public List<Queue> getQueues() {
        return this.queues;
    }

    public void setQueues(List<Queue> queues) {
        this.queues = queues;
    }

    public Producer getProducer() {
        return this.producer;
    }

    public Consumer getConsumer() {
        return this.consumer;
    }

    public Map<String, DeclaredConsumer> getConsumers() {
        return this.consumers;
    }

    public void setConsumers(Map<String, DeclaredConsumer> consumers) {
        this.consumers = consumers;
    }

    public Health getHealth() {
        return this.health;
    }

    public Metrics getMetrics() {
        return this.metrics;
    }

    /** A queue to create on startup. */
    public static class Queue {

        /** Queue name. Maximum 47 characters; must not contain '$', ';', '--' or a quote. */
        private @Nullable String name;

        /** Standard, unlogged or partitioned. */
        private QueueKind kind = QueueKind.STANDARD;

        /** Partition interval, for partitioned queues only. */
        private String partitionInterval = "10000";

        /** Retention interval, for partitioned queues only. */
        private String retentionInterval = "100000";

        /**
         * Whether to create the index PGMQ's grouped (FIFO) reads use. Enable it for every queue
         * consumed with group-ordered=true; without it each grouped poll scans the queue table.
         * Requires PGMQ 1.10.0 or later.
         */
        private boolean fifoIndex;

        /**
         * Whether PGMQ should notify on every insert into this queue, which consumers with
         * wake-up=notify wait for. Enabled at startup unless it already is; false leaves the
         * queue's current setting alone. Requires PGMQ 1.10.0 or later.
         */
        private boolean notifyOnInsert;

        /**
         * With notify-on-insert, send at most one notification per this interval. PGMQ drops the
         * notifications of inserts within it rather than delaying them. Unset keeps the queue's
         * current throttle, or PGMQ's default of 250ms when notifications are being enabled; zero
         * disables throttling.
         */
        private @Nullable Duration notifyThrottle;

        public @Nullable String getName() {
            return this.name;
        }

        public void setName(@Nullable String name) {
            this.name = name;
        }

        public QueueKind getKind() {
            return this.kind;
        }

        public void setKind(QueueKind kind) {
            this.kind = kind;
        }

        public String getPartitionInterval() {
            return this.partitionInterval;
        }

        public void setPartitionInterval(String partitionInterval) {
            this.partitionInterval = partitionInterval;
        }

        public String getRetentionInterval() {
            return this.retentionInterval;
        }

        public void setRetentionInterval(String retentionInterval) {
            this.retentionInterval = retentionInterval;
        }

        public boolean isFifoIndex() {
            return this.fifoIndex;
        }

        public void setFifoIndex(boolean fifoIndex) {
            this.fifoIndex = fifoIndex;
        }

        public boolean isNotifyOnInsert() {
            return this.notifyOnInsert;
        }

        public void setNotifyOnInsert(boolean notifyOnInsert) {
            this.notifyOnInsert = notifyOnInsert;
        }

        public @Nullable Duration getNotifyThrottle() {
            return this.notifyThrottle;
        }

        public void setNotifyThrottle(@Nullable Duration notifyThrottle) {
            this.notifyThrottle = notifyThrottle;
        }
    }

    /** Settings applied to the PGMQ client when sending. */
    public static class Producer {

        /**
         * Most messages per send_batch statement. Longer sendBatch, sendRawBatch and sendMessages
         * lists are split into several statements, which stay atomic: they join the caller's
         * transaction, or run in one of their own. Unset sends every batch as one statement. At
         * least 1 when set.
         */
        private @Nullable Integer maxBatchSize;

        /**
         * Headers added to every message sent, including every message of a batch and messages
         * moved to a dead-letter queue. A header of the same name set on the send wins. Use
         * bracket notation for names containing dots, for example "[app.version]".
         */
        private Map<String, String> defaultHeaders = new LinkedHashMap<>();

        public Map<String, String> getDefaultHeaders() {
            return this.defaultHeaders;
        }

        public void setDefaultHeaders(Map<String, String> defaultHeaders) {
            this.defaultHeaders = defaultHeaders;
        }

        public @Nullable Integer getMaxBatchSize() {
            return this.maxBatchSize;
        }

        public void setMaxBatchSize(@Nullable Integer maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }
    }

    /** Defaults applied to every listener container. */
    public static class Consumer {

        /**
         * Number of concurrent polling loops per container. Each loop holds a JDBC connection while
         * it reads and while its handler runs in a transaction. At least 1.
         */
        private int concurrency = 1;

        /** Maximum messages fetched per poll. At least 1. */
        private int batchSize = 10;

        /**
         * How long a fetched message stays invisible to other consumers. Must cover the handler's
         * worst case - and, with batch-size above 1, the whole batch, since the lease starts at the
         * read. Rounded up to whole seconds. Must be positive.
         */
        private Duration visibilityTimeout = Duration.ofSeconds(30);

        /**
         * Initial delay after an empty poll; doubles on each further empty poll up to
         * max-poll-delay. Also the re-check interval while paused. Must be positive.
         */
        private Duration pollDelay = Duration.ofMillis(200);

        /**
         * Ceiling for the empty-queue backoff, and the wait after a failed poll. Must be at least
         * poll-delay.
         */
        private Duration maxPollDelay = Duration.ofSeconds(5);

        /**
         * Random extra wait, between zero and this, added to every sleep after an empty or failed
         * poll, so instances started together do not poll in synchronised bursts. Not applied to
         * empty polls while long-polling. Zero adds nothing. Must not be negative.
         */
        private Duration pollJitter = Duration.ZERO;

        /**
         * When set, waits inside the database for up to this long instead of polling. Holds one
         * JDBC connection per concurrent consumer for the whole window, so size the pool for at
         * least concurrency connections plus the application's own needs. At least 1s when set.
         */
        private @Nullable Duration longPoll;

        /**
         * How polling loops learn of new messages: POLL, or NOTIFY to also wake up on PGMQ's
         * insert notifications, which turns max-poll-delay into a slow fallback poll. NOTIFY needs
         * PGMQ 1.10.0, pgmq.queues[].notify-on-insert (or enableNotifyInsert) on the queue, and
         * the PostgreSQL JDBC driver, and holds one extra connection per container. Not combinable
         * with long-poll.
         */
        private WakeUp wakeUp = WakeUp.POLL;

        /**
         * How messages are taken off the queue. READ leases them and acknowledges after the
         * handler: at-least-once, two writes per message. TRANSACTIONAL_POP pops them inside the
         * handler's transaction, removing each exactly when the handler commits, with one write
         * per message; requires transactional, and works best with transaction-timeout. POP pops
         * them before the handler runs: at-most-once, a failed message is lost. Both pop modes
         * need acknowledge-mode=delete and rule out group-ordered, long-poll, extend-lease and
         * batch-acknowledgements.
         */
        private ConsumeMode consumeMode = ConsumeMode.READ;

        /**
         * What to do with a message after its handler returns normally: DELETE it, ARCHIVE it, or
         * leave it to the handler (MANUAL, single-message acknowledging handlers only).
         */
        private AcknowledgeMode acknowledgeMode = AcknowledgeMode.DELETE;

        /**
         * Read with PGMQ's grouped reads, so that for each 'x-pgmq-group' header value at most one
         * message is in flight at a time and messages for one key are delivered in order.
         * Requires PGMQ 1.10.0 or later.
         */
        private boolean groupOrdered;

        /**
         * How a grouped read spreads a batch across groups. HEAD returns at most one message per
         * group and is the safest default; GREEDY drains a group at a time; ROUND_ROBIN keeps a
         * busy key from monopolising a batch.
         */
        private GroupReadStrategy groupStrategy = GroupReadStrategy.HEAD;

        /**
         * Terminal action once a message has exhausted max-attempts: DEAD_LETTER (requires
         * dead-letter-queue), ARCHIVE, or REDELIVER, which keeps retrying and archives the message
         * once it is poison.
         */
        private FailureAction failureAction = FailureAction.REDELIVER;

        /**
         * Backoff applied before a failed message becomes visible again, while it still has
         * attempts left. Zero leaves the existing lease to expire instead. Rounded up to whole
         * seconds; must not be negative.
         */
        private Duration retryDelay = Duration.ofSeconds(5);

        /**
         * Factor the retry delay grows by with every failed attempt: the delay after the n-th
         * delivery is retry-delay × retry-multiplier^(n - 1), capped at max-retry-delay. Based on
         * PGMQ's read_ct, so it survives restarts. 1.0 keeps the delay fixed. At least 1.0.
         */
        private double retryMultiplier = 1.0;

        /**
         * Upper bound for the retry delay grown by retry-multiplier. Unset leaves it uncapped.
         * Must not be shorter than retry-delay.
         */
        private @Nullable Duration maxRetryDelay;

        /**
         * Delivery attempts before a message is treated as poison, compared against PGMQ's read_ct
         * so the count survives restarts and is shared by every instance. At least 1.
         */
        private int maxAttempts = 5;

        /**
         * Fully qualified exception classes that no retry can fix. When one appears anywhere in the
         * cause chain of a handler's exception, or of a payload conversion failure, the message
         * skips its remaining attempts and gets the terminal failure-action at once.
         */
        private List<Class<? extends Throwable>> nonRetryableExceptions = new ArrayList<>();

        /**
         * Queue that failed and poisoned messages are moved to. Required for
         * failure-action=dead-letter, must exist when the container starts, and must differ from
         * the consumed queue.
         */
        private @Nullable String deadLetterQueue;

        /**
         * Whether to run the handler and its acknowledgement in one transaction, so business writes
         * and the ack commit or roll back together. Requires a PlatformTransactionManager.
         */
        private boolean transactional;

        /**
         * Bounds each transactional handler invocation: a statement that starts after the deadline
         * fails, the transaction rolls back and the message is retried. Enforced at statement
         * boundaries only. Keep it shorter than visibility-timeout. Rounded up to whole seconds.
         * Unset means no timeout. Requires transactional.
         */
        private @Nullable Duration transactionTimeout;

        /**
         * Whether to keep extending the lease of every message in a polled batch until it is
         * settled, refreshing every third of visibility-timeout.
         */
        private boolean extendLease;

        /**
         * Whether to acknowledge the messages of a polled batch with one statement when the batch
         * is done, instead of one statement per message. Applies to single-message handlers; until
         * the flush each message stays leased, so a crash redelivers it. Cannot be combined with
         * transactional or acknowledge-mode=manual.
         */
        private boolean batchAcknowledgements;

        /**
         * With batch-acknowledgements, flush as soon as this many acknowledgements are pending
         * rather than once per polled batch. Unset flushes once per batch. At least 1 when set.
         */
        private @Nullable Integer ackBatchSize;

        /**
         * How long shutdown waits for in-flight handlers before interrupting them. Keep it below
         * spring.lifecycle.timeout-per-shutdown-phase. Must not be negative.
         */
        private Duration shutdownTimeout = Duration.ofSeconds(30);

        public int getConcurrency() {
            return this.concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public int getBatchSize() {
            return this.batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getVisibilityTimeout() {
            return this.visibilityTimeout;
        }

        public void setVisibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
        }

        public Duration getPollDelay() {
            return this.pollDelay;
        }

        public void setPollDelay(Duration pollDelay) {
            this.pollDelay = pollDelay;
        }

        public Duration getMaxPollDelay() {
            return this.maxPollDelay;
        }

        public void setMaxPollDelay(Duration maxPollDelay) {
            this.maxPollDelay = maxPollDelay;
        }

        public Duration getPollJitter() {
            return this.pollJitter;
        }

        public void setPollJitter(Duration pollJitter) {
            this.pollJitter = pollJitter;
        }

        public @Nullable Duration getLongPoll() {
            return this.longPoll;
        }

        public void setLongPoll(@Nullable Duration longPoll) {
            this.longPoll = longPoll;
        }

        public ConsumeMode getConsumeMode() {
            return this.consumeMode;
        }

        public void setConsumeMode(ConsumeMode consumeMode) {
            this.consumeMode = consumeMode;
        }

        public WakeUp getWakeUp() {
            return this.wakeUp;
        }

        public void setWakeUp(WakeUp wakeUp) {
            this.wakeUp = wakeUp;
        }

        public AcknowledgeMode getAcknowledgeMode() {
            return this.acknowledgeMode;
        }

        public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
            this.acknowledgeMode = acknowledgeMode;
        }

        public boolean isGroupOrdered() {
            return this.groupOrdered;
        }

        public void setGroupOrdered(boolean groupOrdered) {
            this.groupOrdered = groupOrdered;
        }

        public GroupReadStrategy getGroupStrategy() {
            return this.groupStrategy;
        }

        public void setGroupStrategy(GroupReadStrategy groupStrategy) {
            this.groupStrategy = groupStrategy;
        }

        public FailureAction getFailureAction() {
            return this.failureAction;
        }

        public void setFailureAction(FailureAction failureAction) {
            this.failureAction = failureAction;
        }

        public Duration getRetryDelay() {
            return this.retryDelay;
        }

        public void setRetryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
        }

        public double getRetryMultiplier() {
            return this.retryMultiplier;
        }

        public void setRetryMultiplier(double retryMultiplier) {
            this.retryMultiplier = retryMultiplier;
        }

        public @Nullable Duration getMaxRetryDelay() {
            return this.maxRetryDelay;
        }

        public void setMaxRetryDelay(@Nullable Duration maxRetryDelay) {
            this.maxRetryDelay = maxRetryDelay;
        }

        public int getMaxAttempts() {
            return this.maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public List<Class<? extends Throwable>> getNonRetryableExceptions() {
            return this.nonRetryableExceptions;
        }

        public void setNonRetryableExceptions(List<Class<? extends Throwable>> nonRetryableExceptions) {
            this.nonRetryableExceptions = nonRetryableExceptions;
        }

        public @Nullable String getDeadLetterQueue() {
            return this.deadLetterQueue;
        }

        public void setDeadLetterQueue(@Nullable String deadLetterQueue) {
            this.deadLetterQueue = deadLetterQueue;
        }

        public boolean isTransactional() {
            return this.transactional;
        }

        public void setTransactional(boolean transactional) {
            this.transactional = transactional;
        }

        public @Nullable Duration getTransactionTimeout() {
            return this.transactionTimeout;
        }

        public void setTransactionTimeout(@Nullable Duration transactionTimeout) {
            this.transactionTimeout = transactionTimeout;
        }

        public boolean isExtendLease() {
            return this.extendLease;
        }

        public void setExtendLease(boolean extendLease) {
            this.extendLease = extendLease;
        }

        public boolean isBatchAcknowledgements() {
            return this.batchAcknowledgements;
        }

        public void setBatchAcknowledgements(boolean batchAcknowledgements) {
            this.batchAcknowledgements = batchAcknowledgements;
        }

        public @Nullable Integer getAckBatchSize() {
            return this.ackBatchSize;
        }

        public void setAckBatchSize(@Nullable Integer ackBatchSize) {
            this.ackBatchSize = ackBatchSize;
        }

        public Duration getShutdownTimeout() {
            return this.shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }

        ConsumerOptions toOptions() {
            return ConsumerOptions.builder()
                    .concurrency(this.concurrency)
                    .batchSize(this.batchSize)
                    .visibilityTimeout(this.visibilityTimeout)
                    .pollDelay(this.pollDelay)
                    .maxPollDelay(this.maxPollDelay)
                    .pollJitter(this.pollJitter)
                    .longPoll(this.longPoll)
                    .wakeUp(this.wakeUp)
                    .consumeMode(this.consumeMode)
                    .acknowledgeMode(this.acknowledgeMode)
                    .groupOrdered(this.groupOrdered)
                    .groupStrategy(this.groupStrategy)
                    .failureAction(this.failureAction)
                    .retryDelay(this.retryDelay)
                    .retryMultiplier(this.retryMultiplier)
                    .maxRetryDelay(this.maxRetryDelay)
                    .maxAttempts(this.maxAttempts)
                    .nonRetryableExceptions(this.nonRetryableExceptions)
                    .deadLetterQueue(this.deadLetterQueue)
                    .transactional(this.transactional)
                    .transactionTimeout(this.transactionTimeout)
                    .extendLease(this.extendLease)
                    .batchAcknowledgements(this.batchAcknowledgements)
                    .ackBatchSize(this.ackBatchSize)
                    .shutdownTimeout(this.shutdownTimeout)
                    .build();
        }
    }

    /**
     * A listener container created from configuration, under pgmq.consumers.{name}. Every
     * pgmq.consumer.* setting can be overridden here.
     */
    public static class DeclaredConsumer extends Consumer {

        /** Queue to consume. Defaults to the consumer's name. */
        private @Nullable String queue;

        /**
         * Name of the handler bean: a PgmqMessageHandler, PgmqAcknowledgingMessageHandler or
         * PgmqBatchMessageHandler. Required.
         */
        private @Nullable String handler;

        /**
         * Class the payload is converted to. Defaults to the handler's type argument; required
         * when that cannot be determined, as for a lambda bean. Must be assignable to it.
         */
        private @Nullable Class<?> payloadType;

        /**
         * Name of the PlatformTransactionManager bean to use, for transactional processing and
         * atomic dead-lettering. Defaults to the only one, when there is exactly one.
         */
        private @Nullable String transactionManager;

        /** Whether the container starts with the application context. */
        private boolean autoStartup = true;

        public @Nullable String getQueue() {
            return this.queue;
        }

        public void setQueue(@Nullable String queue) {
            this.queue = queue;
        }

        public @Nullable String getHandler() {
            return this.handler;
        }

        public void setHandler(@Nullable String handler) {
            this.handler = handler;
        }

        public @Nullable Class<?> getPayloadType() {
            return this.payloadType;
        }

        public void setPayloadType(@Nullable Class<?> payloadType) {
            this.payloadType = payloadType;
        }

        public @Nullable String getTransactionManager() {
            return this.transactionManager;
        }

        public void setTransactionManager(@Nullable String transactionManager) {
            this.transactionManager = transactionManager;
        }

        public boolean isAutoStartup() {
            return this.autoStartup;
        }

        public void setAutoStartup(boolean autoStartup) {
            this.autoStartup = autoStartup;
        }
    }

    /** Health indicator settings. */
    public static class Health {

        /** Whether to contribute a PGMQ health indicator. */
        private boolean enabled = true;

        /**
         * Queues whose depth is reported by the health indicator. When empty, only PGMQ's presence
         * and version are checked.
         */
        private List<String> queues = new ArrayList<>();

        /**
         * Depth above which a monitored queue makes the health indicator report DOWN. Negative
         * disables the threshold check.
         */
        private long maxQueueDepth = -1;

        public boolean isEnabled() {
            return this.enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public List<String> getQueues() {
            return this.queues;
        }

        public void setQueues(List<String> queues) {
            this.queues = queues;
        }

        public long getMaxQueueDepth() {
            return this.maxQueueDepth;
        }

        public void setMaxQueueDepth(long maxQueueDepth) {
            this.maxQueueDepth = maxQueueDepth;
        }
    }

    /** Metrics settings. */
    public static class Metrics {

        /** Whether to record Micrometer metrics for sends and consumer activity. */
        private boolean enabled = true;

        /** Queues whose depth and oldest-message age are published as gauges. */
        private List<String> queues = new ArrayList<>();

        /**
         * Most often a gauged queue re-queries pgmq.metrics(), which scans the queue table; scrapes
         * in between return the cached values. Must be positive.
         */
        private Duration refreshInterval = Duration.ofSeconds(10);

        /**
         * Per-queue overrides of refresh-interval, for example a small queue whose depth should be
         * nearly live. Every key must be one of the gauged queues. Must be positive.
         */
        private Map<String, Duration> refreshIntervals = new LinkedHashMap<>();

        public boolean isEnabled() {
            return this.enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public List<String> getQueues() {
            return this.queues;
        }

        public void setQueues(List<String> queues) {
            this.queues = queues;
        }

        public Duration getRefreshInterval() {
            return this.refreshInterval;
        }

        public void setRefreshInterval(Duration refreshInterval) {
            this.refreshInterval = refreshInterval;
        }

        public Map<String, Duration> getRefreshIntervals() {
            return this.refreshIntervals;
        }

        public void setRefreshIntervals(Map<String, Duration> refreshIntervals) {
            this.refreshIntervals = refreshIntervals;
        }
    }
}
