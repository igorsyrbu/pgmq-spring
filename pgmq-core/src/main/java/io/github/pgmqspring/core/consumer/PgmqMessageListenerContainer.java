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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.random.RandomGenerator;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.PgmqVersion;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.UnsupportedPgmqFeatureException;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.client.SendOptions;

/**
 * A managed, polling consumer for one PGMQ queue.
 *
 * <p>The container owns {@code concurrency} polling loops. Each loop reads a batch, hands each
 * message (or the whole batch) to the handler, and applies the configured acknowledge or failure
 * action. It is a {@link SmartLifecycle}, so in a Spring application it starts and stops with the
 * context.
 *
 * <h2>The safety invariant</h2>
 *
 * <p>On any <em>unexpected</em> internal failure - a database error, a listener callback blowing
 * up, a bug in this class - the container leaves the message <strong>untouched</strong>. It never
 * deletes or archives a message it is not sure was handled. PGMQ's visibility timeout then
 * redelivers it. The cost of that choice is a possible duplicate delivery, which at-least-once
 * semantics already require handlers to tolerate; the alternative would be silent message loss.
 *
 * <h2>Poison messages</h2>
 *
 * <p>Protection is based on PGMQ's {@code read_ct}, which the database increments on every read.
 * Because the count lives in the row rather than in the consumer, it survives restarts and is
 * shared across every instance of a scaled-out application. Once it exceeds
 * {@link ConsumerOptions.Builder#maxAttempts(int)} the handler is not invoked at all and the
 * terminal action is applied immediately. With {@link ConsumeMode#TRANSACTIONAL_POP}, where
 * {@code pop} does not count, a rolled-back attempt is counted into {@code read_ct} explicitly;
 * with {@link ConsumeMode#POP} a failed message is gone, and nothing is retried.
 *
 * @param <T> the payload type
 */
public class PgmqMessageListenerContainer<T> implements SmartLifecycle, AutoCloseable {

    private static final Log logger = LogFactory.getLog(PgmqMessageListenerContainer.class);

    private static final String MDC_QUEUE = "pgmq.queue";

    private static final String MDC_MESSAGE_ID = "pgmq.messageId";

    private static final String MDC_READ_COUNT = "pgmq.readCount";

    private static final ConsumerListener NO_LISTENER = new ConsumerListener() {
    };

    /** A lease is refreshed every third of the visibility timeout, but never more often than this. */
    private static final long MIN_LEASE_REFRESH_MILLIS = 200;

    /** How much longer than the shutdown timeout a caller waits for a drain already in progress. */
    private static final long STOP_GRACE_MILLIS = 1000;

    private final PgmqOperations pgmq;

    private final String queue;

    private final Class<T> payloadType;

    private final ConsumerOptions options;

    private final @Nullable PgmqAcknowledgingMessageHandler<T> messageHandler;

    private final @Nullable PgmqBatchMessageHandler<T> batchHandler;

    /** Makes dead-lettering atomic; untimed, unlike {@link #handlerTransactionTemplate}. */
    private final @Nullable TransactionTemplate deadLetterTransactionTemplate;

    private final @Nullable TransactionTemplate handlerTransactionTemplate;

    private volatile ConsumerListener listener;

    private final AtomicBoolean running = new AtomicBoolean();

    private final AtomicBoolean paused = new AtomicBoolean();

    private volatile @Nullable ExecutorService executor;

    private volatile @Nullable ScheduledExecutorService leaseExtender;

    private final WakeUpSignal wakeUp = new WakeUpSignal();

    private final @Nullable DataSource notificationDataSource;

    private volatile @Nullable InsertNotificationListener notifications;

    /** With {@link WakeUp#NOTIFY}, wakes the loops when a retry this container scheduled is due. */
    private volatile @Nullable ScheduledExecutorService retryWakeUps;

    /** The seconds, on the {@code nanoTime} scale, that a retry wake-up is already scheduled for. */
    private final Set<Long> scheduledWakeUps = ConcurrentHashMap.newKeySet();

    private volatile @Nullable CountDownLatch stopped;

    private volatile @Nullable CountDownLatch keepAlive;

    /** Released when the most recent stop has finished draining; {@code null} until the first stop. */
    private volatile @Nullable CountDownLatch drained;

    private volatile int phase = DEFAULT_PHASE - 100;

    private volatile boolean autoStartup = true;

    private final boolean verifyQueuesOnStart;

    private String beanName;

    PgmqMessageListenerContainer(Builder<T> builder) {
        this.pgmq = builder.pgmq;
        this.queue = QueueNames.validate(builder.queue);
        this.payloadType = builder.payloadType;
        this.options = builder.options;
        this.messageHandler = builder.messageHandler;
        this.batchHandler = builder.batchHandler;
        this.verifyQueuesOnStart = builder.verifyQueuesOnStart;
        if (this.queue.equals(this.options.getDeadLetterQueue())) {
            throw new IllegalArgumentException("queue '" + this.queue + "' cannot be its own dead-letter queue");
        }
        this.notificationDataSource = notificationDataSource(builder);
        this.listener = builder.listener != null ? builder.listener : NO_LISTENER;
        this.beanName = "pgmqListener-" + this.queue;
        PlatformTransactionManager transactionManager = builder.transactionManager;
        this.deadLetterTransactionTemplate = transactionManager != null
                ? new TransactionTemplate(transactionManager)
                : null;
        this.handlerTransactionTemplate = transactionManager != null
                ? handlerTransactionTemplate(transactionManager, this.options)
                : null;
        Duration transactionTimeout = this.options.getTransactionTimeout();
        if (transactionTimeout != null && transactionTimeout.compareTo(this.options.getVisibilityTimeout()) >= 0
                && this.options.getConsumeMode() == ConsumeMode.READ && !this.options.isExtendLease()) {
            logger.warn("Queue '" + this.queue + "' has a transactionTimeout (" + transactionTimeout
                    + ") no shorter than its visibilityTimeout (" + this.options.getVisibilityTimeout()
                    + "), so a message can be redelivered while its handler's transaction is still open");
        }
        if (this.options.isTransactional() && transactionManager == null) {
            throw new IllegalArgumentException(
                    "transactional processing was requested for queue '" + this.queue
                            + "' but no PlatformTransactionManager was supplied");
        }
        if (this.options.getFailureAction() == FailureAction.DEAD_LETTER && transactionManager == null) {
            logger.warn("Queue '" + this.queue + "' dead-letters without a PlatformTransactionManager. "
                    + "The send to the dead-letter queue and the delete from the source queue cannot be made "
                    + "atomic, so a crash between them can duplicate or strand a message. Supply a "
                    + "transaction manager to close that window.");
        }
    }

    private @Nullable DataSource notificationDataSource(Builder<T> builder) {
        if (this.options.getWakeUp() != WakeUp.NOTIFY) {
            return null;
        }
        DataSource dataSource = builder.notificationDataSource;
        if (dataSource == null && this.pgmq instanceof PgmqTemplate template) {
            dataSource = template.getJdbcTemplate().getDataSource();
        }
        if (dataSource == null) {
            throw new IllegalArgumentException("wakeUp=NOTIFY for queue '" + this.queue + "' needs a DataSource to "
                    + "listen on: use a PgmqTemplate, or set the builder's notificationDataSource");
        }
        if (!InsertNotificationListener.driverPresent()) {
            throw new IllegalArgumentException("wakeUp=NOTIFY for queue '" + this.queue + "' needs the PostgreSQL "
                    + "JDBC driver (org.postgresql:postgresql) on the classpath to receive notifications");
        }
        return dataSource;
    }

    /** Creates a builder for a container reading {@code queue}. */
    public static <T> Builder<T> builder(PgmqOperations pgmq, String queue, Class<T> payloadType) {
        Assert.notNull(pgmq, "pgmq must not be null");
        Assert.notNull(payloadType, "payloadType must not be null");
        return new Builder<>(pgmq, queue, payloadType);
    }

    /** The queue this container consumes. */
    public String getQueue() {
        return this.queue;
    }

    /** The options this container was built with. */
    public ConsumerOptions getOptions() {
        return this.options;
    }

    /**
     * Adds a listener alongside the one given to the builder, unless that same instance is already
     * attached - directly or inside a {@link CompositeConsumerListener}.
     *
     * <p>This is how Spring Boot's metrics auto-configuration attaches its Micrometer listener to
     * every container bean, and why a container that was also given that listener explicitly does
     * not count everything twice. Safe to call at any time; callbacks already in progress keep the
     * previous set.
     */
    public synchronized void addListener(ConsumerListener extra) {
        Assert.notNull(extra, "listener must not be null");
        List<ConsumerListener> current = flatten(this.listener);
        if (current.stream().anyMatch((existing) -> existing == extra)) {
            return;
        }
        List<ConsumerListener> all = new ArrayList<>(current);
        all.add(extra);
        this.listener = CompositeConsumerListener.of(all);
    }

    private static List<ConsumerListener> flatten(ConsumerListener listener) {
        if (listener == NO_LISTENER) {
            return List.of();
        }
        if (listener instanceof CompositeConsumerListener composite) {
            List<ConsumerListener> all = new ArrayList<>();
            composite.getDelegates().forEach((delegate) -> all.addAll(flatten(delegate)));
            return all;
        }
        return List.of(listener);
    }

    /** Sets the name used in thread names and log messages. */
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    // ---------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------

    @Override
    public void start() {
        if (this.running.get()) {
            return;
        }
        // A restart must not overlap the loops of a stop still draining.
        awaitDrain();
        // Outside the monitor: these run JDBC, which would pin a virtual thread on JDK 21-23.
        if (this.verifyQueuesOnStart) {
            verifyQueuesExist();
            if (this.options.getWakeUp() == WakeUp.NOTIFY) {
                verifyInsertNotifications();
            }
        }
        startPolling();
    }

    private synchronized void startPolling() {
        if (this.running.get()) {
            return;
        }
        this.running.set(true);
        try {
            startResources();
        }
        catch (RuntimeException | Error ex) {
            Resources started = beginStop();
            if (started != null) {
                finishStop(started);
            }
            throw ex;
        }
    }

    private void startResources() {
        this.keepAlive = startKeepAlive();
        DataSource notificationDataSource = this.notificationDataSource;
        if (notificationDataSource != null) {
            InsertNotificationListener listening = new InsertNotificationListener(notificationDataSource, this.queue,
                    this.wakeUp::signal, this.beanName + "-notify");
            listening.start();
            this.notifications = listening;
            this.retryWakeUps = Executors.newSingleThreadScheduledExecutor(
                    VirtualThreads.factory(this.beanName + "-wake-"));
        }
        int concurrency = this.options.getConcurrency();
        if (this.options.isExtendLease()) {
            // One refresh thread per polling loop: each loop holds at most one lease at a time, so a
            // refresh delayed by a slow database never makes another batch's refresh late.
            this.leaseExtender = Executors.newScheduledThreadPool(
                    concurrency, VirtualThreads.factory(this.beanName + "-lease-"));
        }
        CountDownLatch latch = new CountDownLatch(concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(
                concurrency, VirtualThreads.factory(this.beanName + "-"));
        this.executor = pool;
        this.stopped = latch;
        int submitted = 0;
        try {
            for (; submitted < concurrency; submitted++) {
                pool.execute(() -> {
                    try {
                        pollLoop();
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }
        }
        finally {
            // Loops that never started must not hold up the drain of the stop that follows.
            for (int i = submitted; i < concurrency; i++) {
                latch.countDown();
            }
        }
        logger.info("Started PGMQ listener for queue '" + this.queue + "' with concurrency " + concurrency
                + (VirtualThreads.available() ? " on virtual threads" : " on platform threads"));
        if (logger.isDebugEnabled()) {
            logger.debug("PGMQ listener for queue '" + this.queue + "' uses " + this.options);
        }
    }

    /**
     * Fails fast when the queue, or its dead-letter queue, does not exist.
     *
     * <p>Without this a typo in a queue name surfaces as a poll error logged every few seconds for
     * ever, and a missing dead-letter queue only when the first message exhausts its attempts - at
     * which point it can never leave the source queue. If the check itself cannot run, for example
     * because the database is still starting, the container starts anyway and keeps retrying.
     */
    private void verifyQueuesExist() {
        String deadLetterQueue = this.options.getDeadLetterQueue();
        List<String> required = deadLetterQueue != null ? List.of(this.queue, deadLetterQueue) : List.of(this.queue);
        for (String name : required) {
            boolean exists;
            try {
                exists = this.pgmq.queueExists(name);
            }
            catch (RuntimeException ex) {
                logger.warn("Could not verify that PGMQ queue '" + name + "' exists; starting the listener anyway. "
                        + "It will keep polling until the database is reachable.", ex);
                return;
            }
            if (!exists) {
                String role = name.equals(this.queue) ? "" : " (the dead-letter queue of '" + this.queue + "')";
                throw new IllegalStateException("PGMQ queue '" + name + "'" + role + " does not exist. Create it "
                        + "first - for example with pgmq.queues[].name, or PgmqOperations.createQueue().");
            }
        }
    }

    /**
     * Fails fast when {@link WakeUp#NOTIFY} cannot work: PGMQ is older than 1.10.0, or does not
     * notify on inserts into this queue - which would otherwise silently leave only the slow
     * fallback poll. Like {@link #verifyQueuesExist()}, starts anyway if the check cannot run.
     */
    private void verifyInsertNotifications() {
        Duration throttle;
        try {
            PgmqCapabilities capabilities = this.pgmq.capabilities();
            capabilities.require(capabilities.insertNotify(), "wakeUp=NOTIFY (pgmq.enable_notify_insert)",
                    new PgmqVersion(1, 10, 0));
            throttle = this.pgmq.notifyInsertThrottle(this.queue);
        }
        catch (UnsupportedPgmqFeatureException ex) {
            throw ex;
        }
        catch (RuntimeException ex) {
            logger.warn("Could not verify that PGMQ notifies on inserts into queue '" + this.queue
                    + "'; starting the listener anyway", ex);
            return;
        }
        if (throttle == null) {
            throw new IllegalStateException("PGMQ queue '" + this.queue + "' is consumed with wakeUp=NOTIFY, but PGMQ "
                    + "does not notify on inserts into it. Enable it first - for example with "
                    + "pgmq.queues[].notify-on-insert, or PgmqOperations.enableNotifyInsert().");
        }
    }

    /**
     * Keeps the JVM alive while the container runs.
     *
     * <p>The polling loops run on virtual threads, which are always daemon threads, or on daemon
     * platform threads. Without this, an application whose only work is consuming - no web server,
     * nothing else non-daemon - would exit as soon as startup finished, with its containers still
     * "running". Other Spring messaging containers behave the same way: while one runs, the
     * application stays up; stopping it, or closing the context, releases the JVM.
     */
    private CountDownLatch startKeepAlive() {
        CountDownLatch alive = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                alive.await();
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, this.beanName + "-keepalive");
        thread.setDaemon(false);
        thread.start();
        return alive;
    }

    @Override
    public void stop() {
        Resources resources = beginStop();
        if (resources != null) {
            finishStop(resources);
            return;
        }
        // Already stopping - typically an asynchronous stop(Runnable) from context shutdown. A
        // blocking stop() must not return while that drain is still running, or its caller would
        // proceed as if in-flight messages were finished.
        awaitDrain();
    }

    /** Waits for the drain of the most recent stop, if one is still in progress. */
    private void awaitDrain() {
        CountDownLatch draining = this.drained;
        if (draining == null || draining.getCount() == 0) {
            return;
        }
        try {
            draining.await(this.options.getShutdownTimeout().toMillis() + STOP_GRACE_MILLIS, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops asynchronously, so that when a context shuts down every container drains in parallel
     * rather than one after another, each waiting up to its own shutdown timeout.
     */
    @Override
    public void stop(Runnable callback) {
        Resources resources = beginStop();
        CountDownLatch draining = this.drained;
        if (resources == null && (draining == null || draining.getCount() == 0)) {
            callback.run();
            return;
        }
        // Even when another stop is already draining, the callback waits for it: the caller - the
        // context closing - would otherwise tear down what the in-flight handlers still use.
        Thread drain = new Thread(() -> {
            try {
                if (resources != null) {
                    finishStop(resources);
                }
                else {
                    awaitDrain();
                }
            }
            finally {
                callback.run();
            }
        }, this.beanName + "-shutdown");
        drain.setDaemon(true);
        drain.start();
    }

    /** Stops polling immediately and captures what must be drained; {@code null} if not running. */
    private synchronized @Nullable Resources beginStop() {
        if (!this.running.compareAndSet(true, false)) {
            return null;
        }
        // Loops waiting out a long poll delay must notice the stop now, not when the delay ends.
        this.wakeUp.signal();
        CountDownLatch drain = new CountDownLatch(1);
        this.drained = drain;
        return new Resources(this.stopped, this.executor, this.leaseExtender, this.keepAlive, this.notifications,
                this.retryWakeUps, drain);
    }

    private void finishStop(Resources resources) {
        try {
            // Graceful drain: polling has stopped, so wait for handlers that are still running.
            // Nothing is acknowledged on their behalf, so a handler that does not finish in time
            // simply leaves its message for redelivery.
            CountDownLatch latch = resources.latch();
            if (latch != null && !latch.await(this.options.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                logger.warn("PGMQ listener for queue '" + this.queue + "' did not drain within "
                        + this.options.getShutdownTimeout() + ". In-flight messages were not acknowledged and "
                        + "will be redelivered once their visibility timeout expires.");
            }
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        finally {
            shutdownNow(resources.pool());
            shutdownNow(resources.leaseExtender());
            shutdownNow(resources.retryWakeUps());
            CountDownLatch keepAlive = resources.keepAlive();
            if (keepAlive != null) {
                keepAlive.countDown();
            }
            InsertNotificationListener notifications = resources.notifications();
            if (notifications != null) {
                notifications.stop();
            }
            synchronized (this) {
                // Only clear what this stop captured: start() may already have run again.
                if (this.executor == resources.pool()) {
                    this.executor = null;
                }
                if (this.leaseExtender == resources.leaseExtender()) {
                    this.leaseExtender = null;
                }
                if (this.notifications == resources.notifications()) {
                    this.notifications = null;
                }
                if (this.retryWakeUps == resources.retryWakeUps()) {
                    this.retryWakeUps = null;
                }
            }
        }
        resources.drained().countDown();
        logger.info("Stopped PGMQ listener for queue '" + this.queue + "'");
    }

    private static void shutdownNow(@Nullable ExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private record Resources(@Nullable CountDownLatch latch, @Nullable ExecutorService pool,
            @Nullable ScheduledExecutorService leaseExtender, @Nullable CountDownLatch keepAlive,
            @Nullable InsertNotificationListener notifications, @Nullable ScheduledExecutorService retryWakeUps,
            CountDownLatch drained) {
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    /** Whether the container starts with the application context; {@code true} by default. */
    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public int getPhase() {
        return this.phase;
    }

    /** Sets the {@link SmartLifecycle} phase; lower values start earlier and stop later. */
    public void setPhase(int phase) {
        this.phase = phase;
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Stops fetching new messages without stopping the container.
     *
     * <p>In-flight handlers run to completion. A poll that was already in flight hands its messages
     * straight back, so pausing never strands messages behind a lease.
     */
    public void pause() {
        if (this.paused.compareAndSet(false, true)) {
            logger.info("Paused PGMQ listener for queue '" + this.queue + "'");
        }
    }

    /** Resumes fetching after {@link #pause()}. */
    public void resume() {
        if (this.paused.compareAndSet(true, false)) {
            this.wakeUp.signal();
            logger.info("Resumed PGMQ listener for queue '" + this.queue + "'");
        }
    }

    /** Whether fetching is currently paused. */
    public boolean isPaused() {
        return this.paused.get();
    }

    // ---------------------------------------------------------------------
    // Poll loop
    // ---------------------------------------------------------------------

    private void pollLoop() {
        Duration backoff = this.options.getPollDelay();
        int consecutiveFailures = 0;
        while (true) {
            // Taken before the running check and the poll, so neither a stop nor a wake-up that
            // arrives meanwhile is missed by the wait that follows.
            long seen = this.wakeUp.generation();
            if (!this.running.get() || Thread.currentThread().isInterrupted()) {
                return;
            }
            try {
                if (this.paused.get()) {
                    this.wakeUp.await(this.options.getPollDelay(), seen);
                    continue;
                }
                boolean found = consume();
                if (consecutiveFailures > 0) {
                    logger.info("PGMQ poll loop for queue '" + this.queue + "' recovered after " + consecutiveFailures
                            + " failed poll(s)");
                    consecutiveFailures = 0;
                }
                if (found || this.options.getLongPoll() != null) {
                    // An empty long poll has already waited, inside the database.
                    backoff = this.options.getPollDelay();
                    continue;
                }
                // After a wake-up, back off from the start again: polling briefly at short
                // intervals also finds messages whose notification PGMQ throttled away.
                boolean wokenUp = this.wakeUp.await(withJitter(backoff), seen);
                backoff = wokenUp ? this.options.getPollDelay() : nextBackoff(backoff);
            }
            catch (Throwable ex) {
                if (interruptedByStop(ex)) {
                    return;
                }
                // The invariant: on any unexpected failure we touch nothing, so whatever was read
                // stays leased and is redelivered when its visibility timeout expires.
                consecutiveFailures++;
                safely(() -> this.listener.onPollError(this.queue, ex));
                String message = "PGMQ poll loop for queue '" + this.queue + "' failed; messages already read will be "
                        + "redelivered after their visibility timeout expires. Retrying in "
                        + this.options.getMaxPollDelay();
                if (consecutiveFailures == 1) {
                    logger.error(message, ex);
                }
                else {
                    // An outage fails every poll of every loop; one stack trace per outage is enough.
                    logger.warn(message + " (" + consecutiveFailures + " consecutive failures, latest: " + ex + ")");
                }
                pauseAfterFailure(withJitter(this.options.getMaxPollDelay()));
            }
        }
    }

    /**
     * Waits out the backoff after a failed poll. Only a stop cuts it short: waking on every
     * notification would turn a persistent failure into a retry per insert.
     */
    private void pauseAfterFailure(Duration pause) {
        long deadline = System.nanoTime() + pause.toNanos();
        while (true) {
            long seen = this.wakeUp.generation();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !this.running.get() || Thread.currentThread().isInterrupted()) {
                return;
            }
            this.wakeUp.await(Duration.ofNanos(remaining), seen);
        }
    }

    /**
     * Whether {@code error} comes from a stop interrupting work that outlasted the shutdown timeout:
     * not a failure of the message, which is left for redelivery.
     */
    private boolean interruptedByStop(Throwable error) {
        if (this.running.get()) {
            return false;
        }
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }

    /** Polls once and handles what the poll returned; whether it returned anything. */
    private boolean consume() {
        if (this.options.getConsumeMode() == ConsumeMode.TRANSACTIONAL_POP) {
            return popInTransaction();
        }
        if (this.options.getConsumeMode() == ConsumeMode.POP) {
            List<PgmqMessage<String>> popped = this.pgmq.pop(this.queue, this.options.getBatchSize(), String.class);
            safely(() -> this.listener.onPolled(this.queue, popped));
            if (popped.isEmpty()) {
                return false;
            }
            dispatchPopped(popped);
            return true;
        }
        List<PgmqMessage<String>> batch = read();
        safely(() -> this.listener.onPolled(this.queue, batch));
        if (batch.isEmpty()) {
            return false;
        }
        if (this.paused.get() || !this.running.get()) {
            // A poll that was already in flight when pause() or stop() was called must not
            // start work. Hand the messages straight back rather than holding their lease
            // until it lapses.
            release(batch, Lease.NONE);
            return true;
        }
        try (Lease lease = openLease(batch)) {
            dispatch(batch, lease);
        }
        return true;
    }

    private List<PgmqMessage<String>> read() {
        ReadOptions readOptions = ReadOptions.defaults()
                .batchSize(this.options.getBatchSize())
                .visibilityTimeout(this.options.getVisibilityTimeout());
        Duration longPoll = this.options.getLongPoll();
        if (longPoll != null) {
            readOptions = readOptions.longPoll(longPoll);
        }
        // Always read the raw document and convert per message afterwards: a payload that cannot be
        // converted must fail that one message - and count towards its attempts - rather than the
        // whole poll, which would redeliver the same batch for ever.
        if (this.options.isGroupOrdered()) {
            return this.pgmq.readGrouped(this.queue, readOptions.groupStrategy(this.options.getGroupStrategy()));
        }
        return this.pgmq.read(this.queue, readOptions);
    }

    /** Makes messages immediately visible again, so pausing or stopping does not strand them. */
    private void release(List<PgmqMessage<String>> batch, Lease lease) {
        List<Long> ids = ids(batch);
        ids.forEach(lease::settle);
        try {
            this.pgmq.setVisibilityTimeout(this.queue, ids, Duration.ZERO);
        }
        catch (Throwable ex) {
            logger.warn("Could not release " + batch.size() + " message(s) on queue '" + this.queue
                    + "' after pause/stop; they will be redelivered once their visibility timeout expires", ex);
        }
    }

    private void dispatch(List<PgmqMessage<String>> batch, Lease lease) {
        if (this.batchHandler != null) {
            dispatchBatch(batch, lease);
            return;
        }
        PendingAcknowledgements pending = this.options.isBatchAcknowledgements()
                ? new PendingAcknowledgements(lease)
                : null;
        try {
            for (int i = 0; i < batch.size(); i++) {
                if (!this.running.get()) {
                    // Shutting down: hand back the messages we have not started rather than beginning
                    // work we may not be able to finish.
                    release(batch.subList(i, batch.size()), lease);
                    return;
                }
                dispatchSingle(batch.get(i), lease, pending);
            }
        }
        finally {
            if (pending != null) {
                pending.flush();
            }
        }
    }

    private void dispatchSingle(PgmqMessage<String> raw, Lease lease, @Nullable PendingAcknowledgements pending) {
        withMdc(raw, () -> {
            if (isPoison(raw)) {
                handlePoison(raw, lease);
                return;
            }
            Instant startedAt = Instant.now();
            PgmqMessage<T> message;
            try {
                message = this.pgmq.convert(raw, this.payloadType);
            }
            catch (Throwable ex) {
                onUnconvertible(raw, ex, lease);
                return;
            }
            DefaultAcknowledgement acknowledgement = new DefaultAcknowledgement(message, lease);
            try {
                if (this.options.isTransactional()) {
                    requireHandlerTransactionTemplate().executeWithoutResult((status) -> {
                        invokeHandler(message, acknowledgement);
                        applyAcknowledgeMode(message, acknowledgement, lease, pending);
                    });
                }
                else {
                    invokeHandler(message, acknowledgement);
                }
            }
            catch (Throwable ex) {
                onHandlerFailure(message, acknowledgement, ex, since(startedAt), lease);
                return;
            }
            if (!this.options.isTransactional()) {
                acknowledgeHandled(List.of(message.id()),
                        () -> applyAcknowledgeMode(message, acknowledgement, lease, pending));
            }
            safely(() -> this.listener.onSuccess(this.queue, message, since(startedAt)));
        });
    }

    private void onHandlerFailure(PgmqMessage<T> message, DefaultAcknowledgement acknowledgement, Throwable error,
            Duration took, Lease lease) {
        if (interruptedByStop(error)) {
            logger.info("Handler for message " + message.id() + " on queue '" + this.queue + "' was interrupted by "
                    + "shutdown; the message will be redelivered");
            return;
        }
        safely(() -> this.listener.onFailure(this.queue, message, took, error));
        if (!this.options.isTransactional() && acknowledgement.isAcknowledged()) {
            // The handler settled the message and then threw. Outside a transaction that
            // settlement has already happened - the message may be deleted, or already in
            // the dead-letter queue - so applying the failure action as well would act on
            // it twice, for example dead-lettering a copy of a message that was deleted.
            logger.warn("Handler for message " + message.id() + " on queue '" + this.queue
                    + "' threw after settling it; the settlement stands and no failure action is applied", error);
            return;
        }
        logger.warn("Handler failed for message " + message.id() + " on queue '" + this.queue
                + "' (read count " + message.readCount() + ")", error);
        applyFailureAction(message, error, lease);
    }

    /**
     * Acknowledges messages whose handler has already succeeded outside a transaction. A failure
     * here is not the handler's: the messages are left for redelivery, as by any unexpected failure,
     * rather than retried, archived or dead-lettered as though their handler had thrown.
     */
    private void acknowledgeHandled(List<Long> ids, Runnable acknowledgement) {
        try {
            acknowledgement.run();
        }
        catch (Throwable ex) {
            logger.warn("Could not acknowledge handled message(s) " + ids + " on queue '" + this.queue
                    + "'; they will be redelivered once their visibility timeout expires", ex);
        }
    }

    private void dispatchBatch(List<PgmqMessage<String>> batch, Lease lease) {
        List<PgmqMessage<T>> deliverable = new ArrayList<>();
        for (PgmqMessage<String> raw : batch) {
            if (isPoison(raw)) {
                withMdc(raw, () -> handlePoison(raw, lease));
                continue;
            }
            try {
                deliverable.add(this.pgmq.convert(raw, this.payloadType));
            }
            catch (Throwable ex) {
                withMdc(raw, () -> onUnconvertible(raw, ex, lease));
            }
        }
        if (deliverable.isEmpty()) {
            return;
        }
        withQueueMdc(() -> handleDeliverableBatch(deliverable, lease));
    }

    private void handleDeliverableBatch(List<PgmqMessage<T>> deliverable, Lease lease) {
        Instant startedAt = Instant.now();
        PgmqBatchMessageHandler<T> handler = this.batchHandler;
        Assert.state(handler != null, "no batch handler");
        try {
            if (this.options.isTransactional()) {
                requireHandlerTransactionTemplate().executeWithoutResult((status) -> {
                    invokeBatchHandler(handler, deliverable);
                    acknowledgeBatch(deliverable, lease);
                });
            }
            else {
                invokeBatchHandler(handler, deliverable);
            }
        }
        catch (Throwable ex) {
            if (interruptedByStop(ex)) {
                logger.info("Batch handler on queue '" + this.queue + "' was interrupted by shutdown; its "
                        + deliverable.size() + " message(s) will be redelivered");
                return;
            }
            Duration took = since(startedAt);
            logger.warn("Batch handler failed for " + deliverable.size() + " messages on queue '" + this.queue + "'",
                    ex);
            for (PgmqMessage<T> message : deliverable) {
                safely(() -> this.listener.onFailure(this.queue, message, took, ex));
                withMdc(message, () -> applyFailureAction(message, ex, lease));
            }
            return;
        }
        if (!this.options.isTransactional()) {
            acknowledgeHandled(ids(deliverable), () -> acknowledgeBatch(deliverable, lease));
        }
        notifySuccess(deliverable, since(startedAt));
    }

    // ---------------------------------------------------------------------
    // Pop modes
    // ---------------------------------------------------------------------

    /**
     * {@link ConsumeMode#TRANSACTIONAL_POP}: pops and handles in one transaction, so the message is
     * removed exactly when the handler's writes commit. A single-message handler gets one message
     * per transaction, because a popped batch cannot be partly rolled back.
     *
     * <p>A popped message that cannot be delivered - poison, or a payload that does not convert -
     * is dead-lettered inside the same transaction when that is its fate, so no other consumer can
     * pop it in between. Otherwise the transaction rolls back, restoring its row, and it is retried
     * or archived exactly as in read mode: a popped row cannot be passed to {@code pgmq.archive}.
     */
    private boolean popInTransaction() {
        int count = this.batchHandler != null ? this.options.getBatchSize() : 1;
        PoppedBatch<T> popped = new PoppedBatch<>();
        try {
            requireHandlerTransactionTemplate().executeWithoutResult((status) -> {
                popped.messages = this.pgmq.pop(this.queue, count, String.class).stream()
                        .map(PgmqMessageListenerContainer::asAttempt)
                        .toList();
                if (popped.messages.isEmpty()) {
                    return;
                }
                if (this.paused.get() || !this.running.get()) {
                    // In flight when pause() or stop() was called: the rollback hands them back.
                    status.setRollbackOnly();
                    return;
                }
                for (PgmqMessage<String> raw : popped.messages) {
                    if (isPoison(raw)) {
                        popped.undeliverable.add(new Undeliverable(raw, null, poisonReason(raw)));
                        continue;
                    }
                    try {
                        popped.deliverable.add(this.pgmq.convert(raw, this.payloadType));
                    }
                    catch (RuntimeException ex) {
                        popped.undeliverable.add(new Undeliverable(raw, ex, failureReason(raw, ex)));
                    }
                }
                if (!popped.undeliverable.isEmpty()) {
                    if (!canDeadLetterInTransaction(popped.undeliverable)) {
                        status.setRollbackOnly();
                        return;
                    }
                    popped.undeliverable.forEach((undeliverable) -> sendToDeadLetterQueue(undeliverable.message(),
                            undeliverable.reason(), undeliverable.error()));
                    popped.deadLetteredInTransaction = true;
                }
                if (!popped.deliverable.isEmpty()) {
                    popped.startedAt = Instant.now();
                    invokeHandlerOnPopped(popped.deliverable);
                }
            });
        }
        catch (RuntimeException | Error ex) {
            if (popped.startedAt == null) {
                // The pop itself failed: a poll error, and nothing was taken.
                throw ex;
            }
            safely(() -> this.listener.onPolled(this.queue, popped.messages));
            onPoppedHandlerFailure(popped, ex);
            return true;
        }
        List<PgmqMessage<String>> messages = popped.messages;
        safely(() -> this.listener.onPolled(this.queue, messages));
        if (messages.isEmpty()) {
            return false;
        }
        for (Undeliverable undeliverable : popped.undeliverable) {
            PgmqMessage<String> raw = undeliverable.message();
            Throwable error = undeliverable.error();
            withMdc(raw, () -> {
                if (popped.deadLetteredInTransaction) {
                    reportUndeliverable(raw, error);
                    reportDeadLettered(raw, undeliverable.reason());
                }
                else if (error == null) {
                    handlePoison(raw, Lease.NONE);
                }
                else {
                    onUnconvertible(raw, error, Lease.NONE);
                }
            });
        }
        Instant startedAt = popped.startedAt;
        if (startedAt != null) {
            notifySuccess(popped.deliverable, since(startedAt));
        }
        return true;
    }

    /** Whether every undeliverable message of a popped batch ends in the dead-letter queue now. */
    private boolean canDeadLetterInTransaction(List<Undeliverable> undeliverable) {
        return this.options.getFailureAction() == FailureAction.DEAD_LETTER
                && undeliverable.stream().allMatch((message) -> message.error() == null
                        || isExhausted(message.message(), message.error()));
    }

    private void reportUndeliverable(PgmqMessage<String> raw, @Nullable Throwable error) {
        if (error == null) {
            safely(() -> this.listener.onPoison(this.queue, raw));
        }
        else {
            safely(() -> this.listener.onFailure(this.queue, raw, Duration.ZERO, error));
        }
    }

    /**
     * A popped message that cannot reach the handler, with the conversion error, or {@code null}
     * when it is poison, and the reason it is dead-lettered for.
     */
    private record Undeliverable(PgmqMessage<String> message, @Nullable Throwable error, String reason) {
    }

    private void invokeHandlerOnPopped(List<PgmqMessage<T>> deliverable) {
        PgmqBatchMessageHandler<T> handler = this.batchHandler;
        if (handler != null) {
            withQueueMdc(() -> invokeBatchHandler(handler, deliverable));
            return;
        }
        PgmqMessage<T> message = deliverable.get(0);
        withMdc(message, () -> invokeHandler(message, new DefaultAcknowledgement(message, Lease.NONE)));
    }

    private void onPoppedHandlerFailure(PoppedBatch<T> popped, Throwable error) {
        if (interruptedByStop(error)) {
            logger.info("Handler on queue '" + this.queue + "' was interrupted by shutdown; the rollback returned its "
                    + popped.deliverable.size() + " popped message(s) to the queue");
            return;
        }
        Instant startedAt = popped.startedAt;
        Duration took = startedAt != null ? since(startedAt) : Duration.ZERO;
        logger.warn("Handler failed for " + popped.deliverable.size() + " popped message(s) on queue '" + this.queue
                + "'; the transaction rolled back", error);
        for (PgmqMessage<T> message : popped.deliverable) {
            safely(() -> this.listener.onFailure(this.queue, message, took, error));
            withMdc(message, () -> applyFailureAction(message, error, Lease.NONE));
        }
    }

    /**
     * A popped message's {@code read_ct} counts its earlier failed attempts, not this delivery:
     * {@code pop} does not increment it. Adding this attempt gives it the meaning it has in read
     * mode, so {@code maxAttempts}, poison handling and the read count handlers see all agree.
     */
    private static PgmqMessage<String> asAttempt(PgmqMessage<String> popped) {
        return new PgmqMessage<>(popped.id(), popped.readCount() + 1, popped.enqueuedAt(), popped.lastReadAt(),
                popped.visibleAt(), popped.headers(), popped.payload(), popped.rawPayload(), popped.queueName());
    }

    /**
     * {@link ConsumeMode#POP}: the poll has already deleted these messages, so they are handled
     * even while pausing or stopping, and a failure loses them - which is what at-most-once means.
     */
    private void dispatchPopped(List<PgmqMessage<String>> batch) {
        PgmqBatchMessageHandler<T> handler = this.batchHandler;
        if (handler != null) {
            List<PgmqMessage<T>> deliverable = new ArrayList<>();
            for (PgmqMessage<String> raw : batch) {
                try {
                    deliverable.add(this.pgmq.convert(raw, this.payloadType));
                }
                catch (RuntimeException ex) {
                    lost(raw, ex, Duration.ZERO);
                }
            }
            if (deliverable.isEmpty()) {
                return;
            }
            Instant startedAt = Instant.now();
            try {
                invokeHandlerOnPopped(deliverable);
                notifySuccess(deliverable, since(startedAt));
            }
            catch (Throwable ex) {
                Duration took = since(startedAt);
                deliverable.forEach((message) -> lost(message, ex, took));
            }
            return;
        }
        for (PgmqMessage<String> raw : batch) {
            withMdc(raw, () -> {
                PgmqMessage<T> message;
                try {
                    message = this.pgmq.convert(raw, this.payloadType);
                }
                catch (RuntimeException ex) {
                    lost(raw, ex, Duration.ZERO);
                    return;
                }
                Instant startedAt = Instant.now();
                try {
                    invokeHandler(message, new DefaultAcknowledgement(message, Lease.NONE));
                    safely(() -> this.listener.onSuccess(this.queue, message, since(startedAt)));
                }
                catch (Throwable ex) {
                    lost(message, ex, since(startedAt));
                }
            });
        }
    }

    private void lost(PgmqMessage<?> message, Throwable error, Duration took) {
        safely(() -> this.listener.onFailure(this.queue, message, took, error));
        logger.error("Message " + message.id() + " on queue '" + this.queue + "' failed and is lost: "
                + "consumeMode=POP removed it before it was handled", error);
    }

    /**
     * A payload that cannot be converted to the container's payload type is a failure of that one
     * message, handled exactly like a handler that threw: it is retried, counts towards
     * {@code maxAttempts}, and then gets the terminal failure action. Retrying is deliberate: the
     * usual cause is a producer and consumer deployed out of step, which a rollback fixes.
     */
    private void onUnconvertible(PgmqMessage<String> raw, Throwable ex, Lease lease) {
        safely(() -> this.listener.onFailure(this.queue, raw, Duration.ZERO, ex));
        logger.warn("Message " + raw.id() + " on queue '" + this.queue + "' could not be converted to "
                + this.payloadType.getName() + " (read count " + raw.readCount() + ")", ex);
        applyFailureAction(raw, ex, lease);
    }

    private void invokeHandler(PgmqMessage<T> message, DefaultAcknowledgement acknowledgement) {
        PgmqAcknowledgingMessageHandler<T> handler = this.messageHandler;
        Assert.state(handler != null, "no message handler");
        try {
            handler.handle(message, acknowledgement);
        }
        catch (RuntimeException | Error ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new HandlerFailedException(ex);
        }
    }

    private void invokeBatchHandler(PgmqBatchMessageHandler<T> handler, List<PgmqMessage<T>> batch) {
        try {
            handler.handle(batch);
        }
        catch (RuntimeException | Error ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new HandlerFailedException(ex);
        }
    }

    // ---------------------------------------------------------------------
    // Acknowledgement and failure handling
    //
    // Every path that settles a message first calls lease.settle(id), so a lease refresh can never
    // land after - and silently undo - a retry delay, and never touches a message already settled.
    // ---------------------------------------------------------------------

    private boolean isPoison(PgmqMessage<?> message) {
        return message.readCount() > this.options.getMaxAttempts();
    }

    private boolean isExhausted(PgmqMessage<?> message, Throwable error) {
        return this.options.isNonRetryable(error) || message.readCount() >= this.options.getMaxAttempts();
    }

    private String poisonReason(PgmqMessage<?> message) {
        return "exceeded maxAttempts=" + this.options.getMaxAttempts() + " (read count " + message.readCount() + ")";
    }

    private String failureReason(PgmqMessage<?> message, Throwable error) {
        Throwable cause = rootCause(error);
        return (this.options.isNonRetryable(error)
                ? "not retryable after "
                : "exhausted maxAttempts=" + this.options.getMaxAttempts() + " after ")
                + cause.getClass().getSimpleName() + (cause.getMessage() != null ? ": " + cause.getMessage() : "");
    }

    private void handlePoison(PgmqMessage<?> message, Lease lease) {
        lease.settle(message.id());
        safely(() -> this.listener.onPoison(this.queue, message));
        String reason = poisonReason(message);
        logger.error("Poison message " + message.id() + " on queue '" + this.queue + "': " + reason);
        try {
            applyTerminalAction(message, reason, null);
        }
        catch (Throwable ex) {
            // Must not abort the rest of the batch. The message stays put and is retried later.
            logger.error("Could not apply failureAction=" + this.options.getFailureAction() + " to poison message "
                    + message.id() + " on queue '" + this.queue + "'; it will be redelivered", ex);
        }
    }

    /**
     * Applies the terminal {@link FailureAction}. {@code REDELIVER} names no terminal state, so a
     * message that reached it would loop for ever; archiving stops the loop without discarding it.
     */
    private void applyTerminalAction(PgmqMessage<?> message, String reason, @Nullable Throwable error) {
        switch (this.options.getFailureAction()) {
            case DEAD_LETTER -> moveToDeadLetterQueue(message, reason, error);
            case ARCHIVE -> {
                logger.error("Archiving message " + message.id() + " from queue '" + this.queue + "': " + reason);
                this.pgmq.archive(this.queue, message.id());
            }
            case REDELIVER -> {
                logger.error("failureAction=REDELIVER has no terminal action for message " + message.id()
                        + " on queue '" + this.queue + "', which " + reason
                        + "; archiving it instead. Configure a dead-letter queue to retain failure details.");
                this.pgmq.archive(this.queue, message.id());
            }
        }
    }

    private void applyAcknowledgeMode(PgmqMessage<T> message, DefaultAcknowledgement acknowledgement, Lease lease,
            @Nullable PendingAcknowledgements pending) {
        if (this.options.getAcknowledgeMode() == AcknowledgeMode.MANUAL || acknowledgement.isAcknowledged()) {
            return;
        }
        if (pending != null) {
            pending.add(message.id());
            return;
        }
        lease.settle(message.id());
        switch (this.options.getAcknowledgeMode()) {
            case DELETE -> this.pgmq.delete(this.queue, message.id());
            case ARCHIVE -> this.pgmq.archive(this.queue, message.id());
            case MANUAL -> {
            }
        }
    }

    private void acknowledgeBatch(List<PgmqMessage<T>> batch, Lease lease) {
        if (this.options.getAcknowledgeMode() == AcknowledgeMode.MANUAL) {
            return;
        }
        List<Long> ids = ids(batch);
        ids.forEach(lease::settle);
        switch (this.options.getAcknowledgeMode()) {
            case DELETE -> this.pgmq.delete(this.queue, ids);
            case ARCHIVE -> this.pgmq.archive(this.queue, ids);
            case MANUAL -> {
            }
        }
    }

    /**
     * Decides what happens to a message whose handler threw.
     *
     * <p>{@link FailureAction} is the <strong>terminal</strong> action, not the action taken on
     * every failure. A message is retried until its attempts are exhausted, and only then is the
     * terminal action applied. Dead-lettering on the very first failure would make
     * {@code maxAttempts} meaningless - except for a non-retryable exception, which no retry can fix.
     */
    private void applyFailureAction(PgmqMessage<?> message, Throwable error, Lease lease) {
        lease.settle(message.id());
        try {
            if (!isExhausted(message, error)) {
                Duration delay = this.options.retryDelayAfter(message.readCount());
                if (this.options.getConsumeMode() == ConsumeMode.TRANSACTIONAL_POP) {
                    // The rollback left the message uncounted and visible at once, even with no delay.
                    this.pgmq.retryAfterRollback(this.queue, message.id(), delay);
                    scheduleWakeUp(delay);
                    return;
                }
                if (!delay.isZero() && !delay.isNegative()) {
                    this.pgmq.setVisibilityTimeout(this.queue, message.id(), delay);
                    scheduleWakeUp(delay);
                }
                // Otherwise leave it untouched: the existing visibility timeout redelivers it.
                return;
            }
            applyTerminalAction(message, failureReason(message, error), error);
        }
        catch (Throwable ex) {
            // Applying the failure action itself failed. Touch nothing: redelivery is the fallback.
            logger.error("Could not apply failureAction=" + this.options.getFailureAction() + " to message "
                    + message.id() + " on queue '" + this.queue + "'; it will be redelivered", ex);
        }
    }

    private void moveToDeadLetterQueue(PgmqMessage<?> message, String reason, @Nullable Throwable error) {
        sendToDeadLetterQueue(message, reason, error);
        reportDeadLettered(message, reason);
    }

    private void reportDeadLettered(PgmqMessage<?> message, String reason) {
        String deadLetterQueue = this.options.getDeadLetterQueue();
        safely(() -> this.listener.onDeadLettered(this.queue, message, String.valueOf(deadLetterQueue), reason));
        logger.error("Dead-lettered message " + message.id() + " from queue '" + this.queue + "' to '"
                + deadLetterQueue + "': " + reason);
    }

    /**
     * Sends a copy with failure headers to the dead-letter queue and deletes the original, in one
     * transaction when a transaction manager is available - joining the caller's, if there is one.
     */
    private void sendToDeadLetterQueue(PgmqMessage<?> message, String reason, @Nullable Throwable error) {
        String deadLetterQueue = this.options.getDeadLetterQueue();
        if (deadLetterQueue == null) {
            throw new IllegalStateException("no dead-letter queue configured for queue '" + this.queue + "'");
        }
        Map<String, Object> headers = new LinkedHashMap<>();
        Map<String, Object> original = message.headers();
        if (original != null) {
            headers.putAll(original);
        }
        headers.put(DeadLetterHeaders.ORIGINAL_QUEUE, this.queue);
        headers.put(DeadLetterHeaders.ORIGINAL_MESSAGE_ID, message.id());
        headers.put(DeadLetterHeaders.ORIGINAL_ENQUEUED_AT, message.enqueuedAt().toString());
        headers.put(DeadLetterHeaders.READ_COUNT, message.readCount());
        headers.put(DeadLetterHeaders.FAILED_AT, Instant.now().toString());
        headers.put(DeadLetterHeaders.REASON, reason);
        if (error != null) {
            headers.put(DeadLetterHeaders.EXCEPTION_TYPE, rootCause(error).getClass().getName());
            String exceptionMessage = rootCause(error).getMessage();
            if (exceptionMessage != null) {
                headers.put(DeadLetterHeaders.EXCEPTION_MESSAGE, truncate(exceptionMessage));
            }
        }
        Runnable move = () -> {
            this.pgmq.sendRaw(deadLetterQueue, message.rawPayload(), SendOptions.headers(headers));
            this.pgmq.delete(this.queue, message.id());
        };
        TransactionTemplate template = this.deadLetterTransactionTemplate;
        if (template != null) {
            // Atomic: the message is never both dead-lettered and left behind.
            template.executeWithoutResult((status) -> move.run());
        }
        else {
            move.run();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private Lease openLease(List<PgmqMessage<String>> batch) {
        ScheduledExecutorService scheduler = this.leaseExtender;
        if (scheduler == null) {
            return Lease.NONE;
        }
        Duration visibility = this.options.getVisibilityTimeout();
        // Refresh at a third of the lease so two refreshes can be missed before it lapses.
        long periodMillis = Math.max(MIN_LEASE_REFRESH_MILLIS, visibility.toMillis() / 3);
        Lease lease = new Lease(ids(batch), true);
        try {
            lease.task = scheduler.scheduleAtFixedRate(() -> lease.refresh(this.pgmq, this.queue, visibility),
                    periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        }
        catch (RejectedExecutionException ex) {
            // Stopping: the dispatch hands back what it does not start, so no refresh is needed.
            return Lease.NONE;
        }
        return lease;
    }

    private static TransactionTemplate handlerTransactionTemplate(PlatformTransactionManager transactionManager,
            ConsumerOptions options) {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        Duration timeout = options.getTransactionTimeout();
        if (timeout != null) {
            long seconds = timeout.getSeconds() + (timeout.getNano() > 0 ? 1 : 0);
            template.setTimeout((int) Math.min(Integer.MAX_VALUE, seconds));
        }
        return template;
    }

    /**
     * With {@link WakeUp#NOTIFY}, wakes the polling loops once a retry becomes visible: making a
     * message visible again sends no notification, so otherwise it would wait for the fallback poll.
     */
    private void scheduleWakeUp(Duration delay) {
        ScheduledExecutorService scheduler = this.retryWakeUps;
        if (scheduler == null) {
            return;
        }
        // PGMQ rounds the delay up to whole seconds.
        long seconds = Math.max(0, delay.getSeconds() + (delay.getNano() > 0 ? 1 : 0));
        // One wake-up per second covers every retry falling due in it, however many fail; it fires
        // once that second has ended, after the last of them.
        long dueSecond = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) + seconds;
        if (!this.scheduledWakeUps.add(dueSecond)) {
            return;
        }
        try {
            scheduler.schedule(() -> {
                this.scheduledWakeUps.remove(dueSecond);
                this.wakeUp.signal();
            }, seconds + 1, TimeUnit.SECONDS);
        }
        catch (RejectedExecutionException ex) {
            // Stopping: nothing is left to wake.
            this.scheduledWakeUps.remove(dueSecond);
        }
    }

    private TransactionTemplate requireHandlerTransactionTemplate() {
        TransactionTemplate template = this.handlerTransactionTemplate;
        Assert.state(template != null, "transactional processing requires a PlatformTransactionManager");
        return template;
    }

    /** Runs {@code action} with the queue, message id and read count in the MDC. */
    private void withMdc(PgmqMessage<?> message, Runnable action) {
        String previousId = MdcSupport.get(MDC_MESSAGE_ID);
        String previousReadCount = MdcSupport.get(MDC_READ_COUNT);
        MdcSupport.put(MDC_MESSAGE_ID, Long.toString(message.id()));
        MdcSupport.put(MDC_READ_COUNT, Integer.toString(message.readCount()));
        try {
            withQueueMdc(action);
        }
        finally {
            restore(MDC_MESSAGE_ID, previousId);
            restore(MDC_READ_COUNT, previousReadCount);
        }
    }

    /** Runs {@code action} with the queue in the MDC - all a batch has, lacking one message id. */
    private void withQueueMdc(Runnable action) {
        String previousQueue = MdcSupport.get(MDC_QUEUE);
        MdcSupport.put(MDC_QUEUE, this.queue);
        try {
            action.run();
        }
        finally {
            restore(MDC_QUEUE, previousQueue);
        }
    }

    private void notifySuccess(List<? extends PgmqMessage<?>> messages, Duration took) {
        messages.forEach((message) -> safely(() -> this.listener.onSuccess(this.queue, message, took)));
    }

    private static Duration since(Instant startedAt) {
        return Duration.between(startedAt, Instant.now());
    }

    private static List<Long> ids(List<? extends PgmqMessage<?>> messages) {
        return messages.stream().map(PgmqMessage::id).toList();
    }

    private static void restore(String key, @Nullable String previous) {
        if (previous != null) {
            MdcSupport.put(key, previous);
        }
        else {
            MdcSupport.remove(key);
        }
    }

    private void safely(Runnable callback) {
        try {
            callback.run();
        }
        catch (Throwable ex) {
            logger.warn("A ConsumerListener callback failed for queue '" + this.queue + "'", ex);
        }
    }

    private Duration nextBackoff(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(this.options.getMaxPollDelay()) > 0 ? this.options.getMaxPollDelay() : doubled;
    }

    private Duration withJitter(Duration base) {
        return jittered(base, this.options.getPollJitter(), ThreadLocalRandom.current());
    }

    /** {@code base} plus a uniformly random extra wait between zero and {@code jitter}, inclusive. */
    static Duration jittered(Duration base, Duration jitter, RandomGenerator random) {
        long jitterMillis = jitter.toMillis();
        if (jitterMillis <= 0) {
            return base;
        }
        return base.plusMillis(random.nextLong(jitterMillis + 1));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof HandlerFailedException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String truncate(String value) {
        return value.length() <= DeadLetterHeaders.MAX_EXCEPTION_MESSAGE_LENGTH
                ? value
                : value.substring(0, DeadLetterHeaders.MAX_EXCEPTION_MESSAGE_LENGTH) + "...";
    }

    /** Wraps a checked exception thrown by a handler. */
    static final class HandlerFailedException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        HandlerFailedException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    /**
     * The visibility lease on one polled batch.
     *
     * <p>Every message of the batch is held from the moment it is read until it is settled, including
     * messages still waiting behind a slow one. Refreshing and settling take the same lock, which
     * gives the one ordering guarantee that matters: once {@link #settle(long)} returns, no refresh
     * for that message is in flight or will ever start, so it cannot overwrite a retry delay the
     * container is about to set.
     *
     * <p>The lock is a {@link ReentrantLock}, not a monitor, because a refresh holds it across a
     * database call. On JDK 21 to 23 a virtual thread blocked on a {@code synchronized} monitor pins
     * its carrier thread; one blocked on a {@code java.util.concurrent} lock parks and frees it, so
     * a slow database cannot starve the application's other virtual threads.
     */
    private static final class Lease implements AutoCloseable {

        /** Used when lease extension is off: settling does nothing and takes no lock. */
        static final Lease NONE = new Lease(List.of(), false);

        private final Set<Long> pending;

        private final boolean active;

        private final ReentrantLock lock = new ReentrantLock();

        private volatile @Nullable ScheduledFuture<?> task;

        /** Guarded by {@link #lock}. */
        private int failedRefreshes;

        private Lease(List<Long> ids, boolean active) {
            this.pending = new LinkedHashSet<>(ids);
            this.active = active;
        }

        void settle(long id) {
            if (!this.active) {
                return;
            }
            this.lock.lock();
            try {
                this.pending.remove(id);
            }
            finally {
                this.lock.unlock();
            }
        }

        void refresh(PgmqOperations pgmq, String queue, Duration visibility) {
            this.lock.lock();
            try {
                if (this.pending.isEmpty()) {
                    return;
                }
                pgmq.setVisibilityTimeout(queue, List.copyOf(this.pending), visibility);
            }
            catch (Throwable ex) {
                // Refreshes run every third of the visibility timeout, so an outage would otherwise
                // log a stack trace several times a lease; the first one is enough.
                this.failedRefreshes++;
                String message = "Could not extend the lease of " + this.pending.size() + " message(s) on queue '"
                        + queue + "'; they may be redelivered while still being processed";
                if (this.failedRefreshes == 1) {
                    logger.warn(message, ex);
                }
                else {
                    logger.debug(message + " (" + this.failedRefreshes + " consecutive failures, latest: " + ex + ")");
                }
                return;
            }
            finally {
                this.lock.unlock();
            }
            this.failedRefreshes = 0;
        }

        @Override
        public void close() {
            ScheduledFuture<?> current = this.task;
            if (current != null) {
                current.cancel(false);
            }
            if (!this.active) {
                return;
            }
            this.lock.lock();
            try {
                this.pending.clear();
            }
            finally {
                this.lock.unlock();
            }
        }
    }

    /**
     * Acknowledgements of one polled batch that have not been sent yet, with
     * {@link ConsumerOptions.Builder#batchAcknowledgements(boolean)}. Used by a single polling loop
     * only, so it needs no synchronization.
     */
    private final class PendingAcknowledgements {

        private final Lease lease;

        private final int flushThreshold;

        private final List<Long> ids = new ArrayList<>();

        private PendingAcknowledgements(Lease lease) {
            this.lease = lease;
            Integer ackBatchSize = PgmqMessageListenerContainer.this.options.getAckBatchSize();
            this.flushThreshold = ackBatchSize != null
                    ? ackBatchSize
                    : PgmqMessageListenerContainer.this.options.getBatchSize();
        }

        void add(long id) {
            this.ids.add(id);
            if (this.ids.size() >= this.flushThreshold) {
                flush();
            }
        }

        /** Never throws: a failed flush leaves the messages leased, and they are redelivered. */
        void flush() {
            if (this.ids.isEmpty()) {
                return;
            }
            List<Long> flushing = List.copyOf(this.ids);
            this.ids.clear();
            // Settled only now, not when each handler returned: until its delete is sent a message
            // must stay covered by the lease, or it could lapse and reach another consumer first.
            flushing.forEach(this.lease::settle);
            PgmqOperations pgmq = PgmqMessageListenerContainer.this.pgmq;
            String queue = PgmqMessageListenerContainer.this.queue;
            try {
                switch (PgmqMessageListenerContainer.this.options.getAcknowledgeMode()) {
                    case DELETE -> pgmq.delete(queue, flushing);
                    case ARCHIVE -> pgmq.archive(queue, flushing);
                    case MANUAL -> {
                    }
                }
            }
            catch (Throwable ex) {
                logger.warn("Could not acknowledge " + flushing.size() + " handled message(s) on queue '" + queue
                        + "'; they will be redelivered once their visibility timeout expires", ex);
            }
        }
    }

    /** What one transactional pop took and how far it got, kept outside the transaction for the failure path. */
    private static final class PoppedBatch<T> {

        private List<PgmqMessage<String>> messages = List.of();

        private final List<PgmqMessage<T>> deliverable = new ArrayList<>();

        private final List<Undeliverable> undeliverable = new ArrayList<>();

        private boolean deadLetteredInTransaction;

        /** When the handler was invoked; {@code null} until then. */
        private @Nullable Instant startedAt;
    }

    /** Default {@link Acknowledgement}, recording which terminal action the handler chose. */
    private final class DefaultAcknowledgement implements Acknowledgement {

        private final PgmqMessage<T> message;

        private final Lease lease;

        private volatile boolean acknowledged;

        private DefaultAcknowledgement(PgmqMessage<T> message, Lease lease) {
            this.message = message;
            this.lease = lease;
        }

        @Override
        public void acknowledge() {
            settle(() -> PgmqMessageListenerContainer.this.pgmq.delete(
                    PgmqMessageListenerContainer.this.queue, this.message.id()));
        }

        @Override
        public void archive() {
            settle(() -> PgmqMessageListenerContainer.this.pgmq.archive(
                    PgmqMessageListenerContainer.this.queue, this.message.id()));
        }

        @Override
        public void retryLater(Duration delay) {
            Assert.isTrue(delay != null && !delay.isNegative(), "delay must not be negative");
            settle(() -> PgmqMessageListenerContainer.this.pgmq.setVisibilityTimeout(
                    PgmqMessageListenerContainer.this.queue, this.message.id(), delay));
            scheduleWakeUp(delay);
        }

        @Override
        public void retryAt(Instant visibleAt) {
            Assert.notNull(visibleAt, "visibleAt must not be null");
            settle(() -> PgmqMessageListenerContainer.this.pgmq.setVisibleAt(
                    PgmqMessageListenerContainer.this.queue, this.message.id(), visibleAt));
            scheduleWakeUp(Duration.between(Instant.now(), visibleAt));
        }

        @Override
        public void deadLetter(String reason) {
            settle(() -> moveToDeadLetterQueue(this.message, reason, null));
        }

        @Override
        public boolean isAcknowledged() {
            return this.acknowledged;
        }

        /**
         * Applies one settlement. The flag is set only once it has taken effect, so a settlement
         * that itself fails - no dead-letter queue, a database error - leaves the message to the
         * container's failure handling rather than being mistaken for a decision.
         */
        private void settle(Runnable action) {
            if (this.acknowledged) {
                throw new IllegalStateException("message " + this.message.id() + " has already been acknowledged");
            }
            this.lease.settle(this.message.id());
            action.run();
            this.acknowledged = true;
        }
    }

    /**
     * Builds a {@link PgmqMessageListenerContainer}.
     *
     * @param <T> the payload type
     */
    public static final class Builder<T> {

        private final PgmqOperations pgmq;

        private final String queue;

        private final Class<T> payloadType;

        private ConsumerOptions options = ConsumerOptions.defaults();

        private @Nullable PgmqAcknowledgingMessageHandler<T> messageHandler;

        private @Nullable PgmqBatchMessageHandler<T> batchHandler;

        private @Nullable PlatformTransactionManager transactionManager;

        private @Nullable ConsumerListener listener;

        private boolean verifyQueuesOnStart = true;

        private @Nullable DataSource notificationDataSource;

        private boolean acknowledging;

        private Builder(PgmqOperations pgmq, String queue, Class<T> payloadType) {
            this.pgmq = pgmq;
            this.queue = queue;
            this.payloadType = payloadType;
        }

        public Builder<T> options(ConsumerOptions value) {
            this.options = value;
            return this;
        }

        /** Handles one message at a time. */
        public Builder<T> handler(PgmqMessageHandler<T> value) {
            this.messageHandler = (message, acknowledgement) -> value.handle(message);
            this.acknowledging = false;
            return this;
        }

        /**
         * Handles one message at a time and decides its fate itself.
         *
         * <p>Named differently from {@link #handler(PgmqMessageHandler)} on purpose: two
         * overloads taking different functional interfaces would force every caller to cast
         * their lambda to disambiguate.
         */
        public Builder<T> acknowledgingHandler(PgmqAcknowledgingMessageHandler<T> value) {
            this.messageHandler = value;
            this.acknowledging = true;
            return this;
        }

        /** Handles a whole batch at a time. */
        public Builder<T> batchHandler(PgmqBatchMessageHandler<T> value) {
            this.batchHandler = value;
            return this;
        }

        /** Required for transactional processing and for atomic dead-lettering. */
        public Builder<T> transactionManager(@Nullable PlatformTransactionManager value) {
            this.transactionManager = value;
            return this;
        }

        /** Observability hook. */
        public Builder<T> listener(@Nullable ConsumerListener value) {
            this.listener = value;
            return this;
        }

        /**
         * Whether {@code start()} fails when the queue or its dead-letter queue does not exist;
         * {@code true} by default. Turn it off only when the queue is created by something that
         * may run after this container starts.
         */
        public Builder<T> verifyQueuesOnStart(boolean value) {
            this.verifyQueuesOnStart = value;
            return this;
        }

        /**
         * The {@code DataSource} that {@link WakeUp#NOTIFY} takes its listening connection from.
         * Defaults to the client's own when it is a {@code PgmqTemplate}; must reach the same
         * database. Ignored with {@link WakeUp#POLL}.
         */
        public Builder<T> notificationDataSource(@Nullable DataSource value) {
            this.notificationDataSource = value;
            return this;
        }

        public PgmqMessageListenerContainer<T> build() {
            if (this.messageHandler == null && this.batchHandler == null) {
                throw new IllegalArgumentException("a handler or batchHandler is required");
            }
            if (this.messageHandler != null && this.batchHandler != null) {
                throw new IllegalArgumentException("set either handler or batchHandler, not both");
            }
            if (this.acknowledging && this.messageHandler != null
                    && this.options.getConsumeMode() != ConsumeMode.READ) {
                throw new IllegalArgumentException("consumeMode=" + this.options.getConsumeMode() + " cannot be "
                        + "combined with an acknowledgingHandler: a popped message is removed by the pop, so there is "
                        + "nothing left to acknowledge, archive or retry. Use handler(...) instead.");
            }
            if (this.batchHandler != null && this.options.getAcknowledgeMode() == AcknowledgeMode.MANUAL) {
                // A batch handler is given no Acknowledgement, so MANUAL would mean nothing is
                // ever acknowledged and every batch is redelivered forever. Fail loudly instead.
                throw new IllegalArgumentException(
                        "acknowledgeMode=MANUAL cannot be combined with a batchHandler, because a batch "
                                + "handler receives no Acknowledgement. Use a single-message "
                                + "acknowledgingHandler for manual control, or a different acknowledge mode.");
            }
            return new PgmqMessageListenerContainer<>(this);
        }
    }
}
