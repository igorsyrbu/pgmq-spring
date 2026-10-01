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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.client.PgmqOperations;
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
 * terminal action is applied immediately.
 *
 * @param <T> the payload type
 */
public class PgmqMessageListenerContainer<T> implements SmartLifecycle, AutoCloseable {

    private static final Log logger = LogFactory.getLog(PgmqMessageListenerContainer.class);

    private static final String MDC_QUEUE = "pgmq.queue";

    private static final String MDC_MESSAGE_ID = "pgmq.messageId";

    private static final String MDC_READ_COUNT = "pgmq.readCount";

    private final PgmqOperations pgmq;

    private final String queue;

    private final Class<T> payloadType;

    private final ConsumerOptions options;

    private final @Nullable PgmqAcknowledgingMessageHandler<T> messageHandler;

    private final @Nullable PgmqBatchMessageHandler<T> batchHandler;

    private final @Nullable TransactionTemplate transactionTemplate;

    private static final ConsumerListener NO_LISTENER = new ConsumerListener() {
    };

    private volatile ConsumerListener listener;

    private final AtomicBoolean running = new AtomicBoolean();

    private final AtomicBoolean paused = new AtomicBoolean();

    private volatile @Nullable ExecutorService executor;

    private volatile @Nullable ScheduledExecutorService leaseExtender;

    private volatile @Nullable CountDownLatch stopped;

    private volatile @Nullable CountDownLatch keepAlive;

    /** Released when the most recent stop has finished draining; {@code null} until the first stop. */
    private volatile @Nullable CountDownLatch drained;

    private int phase = DEFAULT_PHASE - 100;

    private boolean autoStartup = true;

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
        this.listener = builder.listener != null ? builder.listener : NO_LISTENER;
        this.beanName = "pgmqListener-" + this.queue;
        PlatformTransactionManager transactionManager = builder.transactionManager;
        this.transactionTemplate = transactionManager != null ? new TransactionTemplate(transactionManager) : null;
        if (this.options.isTransactional() && this.transactionTemplate == null) {
            throw new IllegalArgumentException(
                    "transactional processing was requested for queue '" + this.queue
                            + "' but no PlatformTransactionManager was supplied");
        }
        if (this.options.getFailureAction() == FailureAction.DEAD_LETTER && this.transactionTemplate == null) {
            logger.warn("Queue '" + this.queue + "' dead-letters without a PlatformTransactionManager. "
                    + "The send to the dead-letter queue and the delete from the source queue cannot be made "
                    + "atomic, so a crash between them can duplicate or strand a message. Supply a "
                    + "transaction manager to close that window.");
        }
    }

    /** Creates a builder for a container reading {@code queue}. */
    public static <T> Builder<T> builder(PgmqOperations pgmq, String queue, Class<T> payloadType) {
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
    public synchronized void start() {
        if (this.running.get()) {
            return;
        }
        if (this.verifyQueuesOnStart) {
            verifyQueuesExist();
        }
        this.running.set(true);
        this.keepAlive = startKeepAlive();
        int concurrency = this.options.getConcurrency();
        CountDownLatch latch = new CountDownLatch(concurrency);
        this.stopped = latch;
        ExecutorService pool = Executors.newFixedThreadPool(
                concurrency, VirtualThreads.factory(this.beanName + "-"));
        this.executor = pool;
        if (this.options.isExtendLease()) {
            // One refresh thread per polling loop: each loop holds at most one lease at a time, so a
            // refresh delayed by a slow database never makes another batch's refresh late.
            this.leaseExtender = Executors.newScheduledThreadPool(
                    concurrency, VirtualThreads.factory(this.beanName + "-lease-"));
        }
        for (int i = 0; i < concurrency; i++) {
            pool.execute(() -> {
                try {
                    pollLoop();
                }
                finally {
                    latch.countDown();
                }
            });
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
        CountDownLatch draining = this.drained;
        if (draining != null) {
            try {
                draining.await(this.options.getShutdownTimeout().toMillis() + 1000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Stops asynchronously, so that when a context shuts down every container drains in parallel
     * rather than one after another, each waiting up to its own shutdown timeout.
     */
    @Override
    public void stop(Runnable callback) {
        Resources resources = beginStop();
        if (resources == null) {
            callback.run();
            return;
        }
        Thread drain = new Thread(() -> {
            try {
                finishStop(resources);
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
        CountDownLatch drain = new CountDownLatch(1);
        this.drained = drain;
        return new Resources(this.stopped, this.executor, this.leaseExtender, this.keepAlive, drain);
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
            if (resources.pool() != null) {
                resources.pool().shutdownNow();
            }
            if (resources.leaseExtender() != null) {
                resources.leaseExtender().shutdownNow();
            }
            if (resources.keepAlive() != null) {
                resources.keepAlive().countDown();
            }
            synchronized (this) {
                // Only clear what this stop captured: start() may already have run again.
                if (this.executor == resources.pool()) {
                    this.executor = null;
                }
                if (this.leaseExtender == resources.leaseExtender()) {
                    this.leaseExtender = null;
                }
            }
        }
        resources.drained().countDown();
        logger.info("Stopped PGMQ listener for queue '" + this.queue + "'");
    }

    private record Resources(@Nullable CountDownLatch latch, @Nullable ExecutorService pool,
            @Nullable ScheduledExecutorService leaseExtender, @Nullable CountDownLatch keepAlive,
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
        while (this.running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (this.paused.get()) {
                    sleep(this.options.getPollDelay());
                    continue;
                }
                List<PgmqMessage<String>> batch = poll();
                if (consecutiveFailures > 0) {
                    logger.info("PGMQ poll loop for queue '" + this.queue + "' recovered after " + consecutiveFailures
                            + " failed poll(s)");
                    consecutiveFailures = 0;
                }
                safely(() -> this.listener.onPolled(this.queue, batch));
                if (batch.isEmpty()) {
                    sleep(backoff);
                    backoff = nextBackoff(backoff);
                    continue;
                }
                backoff = this.options.getPollDelay();
                if (this.paused.get() || !this.running.get()) {
                    // A poll that was already in flight when pause() or stop() was called must not
                    // start work. Hand the messages straight back rather than holding their lease
                    // until it lapses.
                    release(batch, Lease.NONE);
                    continue;
                }
                try (Lease lease = openLease(batch)) {
                    dispatch(batch, lease);
                }
            }
            catch (Throwable ex) {
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
                sleep(this.options.getMaxPollDelay());
            }
        }
    }

    private List<PgmqMessage<String>> poll() {
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
        List<Long> ids = batch.stream().map(PgmqMessage::id).toList();
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
        for (int i = 0; i < batch.size(); i++) {
            if (!this.running.get()) {
                // Shutting down: hand back the messages we have not started rather than beginning
                // work we may not be able to finish.
                release(batch.subList(i, batch.size()), lease);
                return;
            }
            dispatchSingle(batch.get(i), lease);
        }
    }

    private void dispatchSingle(PgmqMessage<String> raw, Lease lease) {
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
                    requireTransactionTemplate().executeWithoutResult((status) -> {
                        invokeHandler(message, acknowledgement);
                        applyAcknowledgeMode(message, acknowledgement, lease);
                    });
                }
                else {
                    invokeHandler(message, acknowledgement);
                    applyAcknowledgeMode(message, acknowledgement, lease);
                }
                Duration took = Duration.between(startedAt, Instant.now());
                safely(() -> this.listener.onSuccess(this.queue, message, took));
            }
            catch (Throwable ex) {
                Duration took = Duration.between(startedAt, Instant.now());
                safely(() -> this.listener.onFailure(this.queue, message, took, ex));
                if (!this.options.isTransactional() && acknowledgement.isAcknowledged()) {
                    // The handler settled the message and then threw. Outside a transaction that
                    // settlement has already happened - the message may be deleted, or already in
                    // the dead-letter queue - so applying the failure action as well would act on
                    // it twice, for example dead-lettering a copy of a message that was deleted.
                    logger.warn("Handler for message " + message.id() + " on queue '" + this.queue
                            + "' threw after settling it; the settlement stands and no failure action is applied", ex);
                    return;
                }
                logger.warn("Handler failed for message " + message.id() + " on queue '" + this.queue
                        + "' (read count " + message.readCount() + ")", ex);
                applyFailureAction(message, ex, lease);
            }
        });
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
        // A batch has no single message id or read count, but its log lines still carry the queue,
        // as a single-message handler's do.
        String previousQueue = MdcSupport.get(MDC_QUEUE);
        MdcSupport.put(MDC_QUEUE, this.queue);
        try {
            handleDeliverableBatch(deliverable, lease);
        }
        finally {
            restore(MDC_QUEUE, previousQueue);
        }
    }

    private void handleDeliverableBatch(List<PgmqMessage<T>> deliverable, Lease lease) {
        Instant startedAt = Instant.now();
        try {
            PgmqBatchMessageHandler<T> handler = this.batchHandler;
            Assert.state(handler != null, "no batch handler");
            if (this.options.isTransactional()) {
                requireTransactionTemplate().executeWithoutResult((status) -> {
                    invokeBatchHandler(handler, deliverable);
                    acknowledgeBatch(deliverable, lease);
                });
            }
            else {
                invokeBatchHandler(handler, deliverable);
                acknowledgeBatch(deliverable, lease);
            }
            Duration took = Duration.between(startedAt, Instant.now());
            deliverable.forEach((message) -> safely(() -> this.listener.onSuccess(this.queue, message, took)));
        }
        catch (Throwable ex) {
            Duration took = Duration.between(startedAt, Instant.now());
            logger.warn("Batch handler failed for " + deliverable.size() + " messages on queue '" + this.queue + "'",
                    ex);
            for (PgmqMessage<T> message : deliverable) {
                safely(() -> this.listener.onFailure(this.queue, message, took, ex));
                withMdc(message, () -> applyFailureAction(message, ex, lease));
            }
        }
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

    private void handlePoison(PgmqMessage<?> message, Lease lease) {
        lease.settle(message.id());
        safely(() -> this.listener.onPoison(this.queue, message));
        String reason = "exceeded maxAttempts=" + this.options.getMaxAttempts() + " (read count "
                + message.readCount() + ")";
        logger.error("Poison message " + message.id() + " on queue '" + this.queue + "': " + reason);
        try {
            switch (this.options.getFailureAction()) {
                case DEAD_LETTER -> deadLetter(message, reason, null);
                case ARCHIVE -> this.pgmq.archive(this.queue, message.id());
                case REDELIVER -> {
                    // REDELIVER has no terminal state, so a poisoned message would loop forever.
                    // Archiving is the least-bad option: it stops the loop without discarding data.
                    logger.error("failureAction=REDELIVER cannot terminate poison message " + message.id()
                            + " on queue '" + this.queue + "'; archiving it instead. Configure a dead-letter "
                            + "queue to retain failure details.");
                    this.pgmq.archive(this.queue, message.id());
                }
            }
        }
        catch (Throwable ex) {
            // Must not abort the rest of the batch. The message stays put and is retried later.
            logger.error("Could not apply failureAction=" + this.options.getFailureAction() + " to poison message "
                    + message.id() + " on queue '" + this.queue + "'; it will be redelivered", ex);
        }
    }

    private void applyAcknowledgeMode(PgmqMessage<T> message, DefaultAcknowledgement acknowledgement, Lease lease) {
        if (this.options.getAcknowledgeMode() == AcknowledgeMode.MANUAL || acknowledgement.isAcknowledged()) {
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
        List<Long> ids = batch.stream().map(PgmqMessage::id).toList();
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
     * {@code maxAttempts} meaningless.
     */
    private void applyFailureAction(PgmqMessage<?> message, Throwable error, Lease lease) {
        lease.settle(message.id());
        boolean exhausted = message.readCount() >= this.options.getMaxAttempts();
        try {
            if (!exhausted) {
                Duration delay = this.options.getRetryDelay();
                if (!delay.isZero() && !delay.isNegative()) {
                    this.pgmq.setVisibilityTimeout(this.queue, message.id(), delay);
                }
                // Otherwise leave it untouched: the existing visibility timeout redelivers it.
                return;
            }
            Throwable cause = rootCause(error);
            String reason = "exhausted maxAttempts=" + this.options.getMaxAttempts() + " after "
                    + cause.getClass().getSimpleName()
                    + (cause.getMessage() != null ? ": " + cause.getMessage() : "");
            switch (this.options.getFailureAction()) {
                case DEAD_LETTER -> deadLetter(message, reason, error);
                case ARCHIVE -> {
                    logger.error("Archiving message " + message.id() + " from queue '" + this.queue + "': "
                            + reason);
                    this.pgmq.archive(this.queue, message.id());
                }
                case REDELIVER -> {
                    // REDELIVER names no terminal state, so an always-failing message would loop
                    // forever. Archiving stops the loop without discarding the payload.
                    logger.error("failureAction=REDELIVER has no terminal action for message " + message.id()
                            + " on queue '" + this.queue + "' which " + reason
                            + "; archiving it instead. Configure a dead-letter queue to retain failure details.");
                    this.pgmq.archive(this.queue, message.id());
                }
            }
        }
        catch (Throwable ex) {
            // Applying the failure action itself failed. Touch nothing: redelivery is the fallback.
            logger.error("Could not apply failureAction=" + this.options.getFailureAction() + " to message "
                    + message.id() + " on queue '" + this.queue + "'; it will be redelivered", ex);
        }
    }

    private void deadLetter(PgmqMessage<?> message, String reason, @Nullable Throwable error) {
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
        TransactionTemplate template = this.transactionTemplate;
        if (template != null) {
            // Atomic: the message is never both dead-lettered and left behind.
            template.executeWithoutResult((status) -> move.run());
        }
        else {
            move.run();
        }
        safely(() -> this.listener.onDeadLettered(this.queue, message, deadLetterQueue, reason));
        logger.error("Dead-lettered message " + message.id() + " from queue '" + this.queue + "' to '"
                + deadLetterQueue + "': " + reason);
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
        long periodMillis = Math.max(200, visibility.toMillis() / 3);
        Lease lease = new Lease(batch.stream().map(PgmqMessage::id).toList(), true);
        lease.task = scheduler.scheduleAtFixedRate(() -> lease.refresh(this.pgmq, this.queue, visibility),
                periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        return lease;
    }

    private TransactionTemplate requireTransactionTemplate() {
        TransactionTemplate template = this.transactionTemplate;
        Assert.state(template != null, "transactional processing requires a PlatformTransactionManager");
        return template;
    }

    private void withMdc(PgmqMessage<?> message, Runnable action) {
        // Every log line emitted by the handler carries enough context to debug a failure:
        // which queue, which message, and how many times it has been tried.
        String previousQueue = MdcSupport.get(MDC_QUEUE);
        String previousId = MdcSupport.get(MDC_MESSAGE_ID);
        String previousReadCount = MdcSupport.get(MDC_READ_COUNT);
        MdcSupport.put(MDC_QUEUE, this.queue);
        MdcSupport.put(MDC_MESSAGE_ID, Long.toString(message.id()));
        MdcSupport.put(MDC_READ_COUNT, Integer.toString(message.readCount()));
        try {
            action.run();
        }
        finally {
            restore(MDC_QUEUE, previousQueue);
            restore(MDC_MESSAGE_ID, previousId);
            restore(MDC_READ_COUNT, previousReadCount);
        }
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

    private static void sleep(Duration duration) {
        if (duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException ex) {
            // Restore the flag: the poll loop checks it and exits. Nothing else is shared state,
            // so an interrupt cannot leave the container half-stopped.
            Thread.currentThread().interrupt();
        }
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
                logger.warn("Could not extend the lease of " + this.pending.size() + " message(s) on queue '" + queue
                        + "'; they may be redelivered while still being processed", ex);
            }
            finally {
                this.lock.unlock();
            }
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
        }

        @Override
        public void retryAt(Instant visibleAt) {
            Assert.notNull(visibleAt, "visibleAt must not be null");
            settle(() -> PgmqMessageListenerContainer.this.pgmq.setVisibleAt(
                    PgmqMessageListenerContainer.this.queue, this.message.id(), visibleAt));
        }

        @Override
        public void deadLetter(String reason) {
            settle(() -> PgmqMessageListenerContainer.this.deadLetter(this.message, reason, null));
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

        public PgmqMessageListenerContainer<T> build() {
            if (this.messageHandler == null && this.batchHandler == null) {
                throw new IllegalArgumentException("a handler or batchHandler is required");
            }
            if (this.messageHandler != null && this.batchHandler != null) {
                throw new IllegalArgumentException("set either handler or batchHandler, not both");
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
