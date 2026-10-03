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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Holds one connection that {@code LISTEN}s for a queue's insert notifications and runs a callback
 * for each batch of them, reconnecting with backoff whenever the connection is lost.
 *
 * <p>Notifications sent while it is reconnecting are lost, so the callback also runs after every
 * (re)connect, and the container's regular poll stays on as a fallback. The connection is taken
 * from the pool for as long as the container runs.
 */
final class InsertNotificationListener {

    private static final Log logger = LogFactory.getLog(InsertNotificationListener.class);

    /** How long one wait for notifications blocks, and so how quickly a stop is noticed. */
    private static final int RECEIVE_TIMEOUT_MILLIS = 1000;

    private static final Duration FIRST_RECONNECT_DELAY = Duration.ofSeconds(1);

    private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(30);

    private final DataSource dataSource;

    private final String queue;

    private final Runnable onNotification;

    private final String threadName;

    private volatile boolean running;

    private volatile @Nullable Thread thread;

    InsertNotificationListener(DataSource dataSource, String queue, Runnable onNotification, String threadName) {
        this.dataSource = dataSource;
        this.queue = queue;
        this.onNotification = onNotification;
        this.threadName = threadName;
    }

    static boolean driverPresent() {
        try {
            Class.forName("org.postgresql.PGConnection", false, InsertNotificationListener.class.getClassLoader());
            return true;
        }
        catch (ClassNotFoundException ex) {
            return false;
        }
    }

    void start() {
        this.running = true;
        Thread listening = new Thread(this::listen, this.threadName);
        listening.setDaemon(true);
        this.thread = listening;
        listening.start();
    }

    void stop() {
        this.running = false;
        Thread listening = this.thread;
        if (listening == null) {
            return;
        }
        listening.interrupt();
        try {
            listening.join(RECEIVE_TIMEOUT_MILLIS * 2L);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void listen() {
        String channel = PgmqOperations.notifyInsertChannel(this.queue);
        Duration reconnectDelay = FIRST_RECONNECT_DELAY;
        int failures = 0;
        while (this.running) {
            Connection connection = null;
            try {
                connection = this.dataSource.getConnection();
                PGConnection notifications = connection.unwrap(PGConnection.class);
                if (!connection.getAutoCommit()) {
                    // LISTEN takes effect at commit; on a connection that never commits it never would.
                    connection.setAutoCommit(true);
                }
                execute(connection, "listen \"" + channel + "\"");
                if (failures > 0) {
                    logger.info("Listening for insert notifications on queue '" + this.queue + "' again after "
                            + failures + " failed attempt(s)");
                }
                failures = 0;
                reconnectDelay = FIRST_RECONNECT_DELAY;
                notifyOwner();
                while (this.running) {
                    PGNotification[] received = notifications.getNotifications(RECEIVE_TIMEOUT_MILLIS);
                    if (received != null && received.length > 0) {
                        notifyOwner();
                    }
                }
            }
            catch (SQLException | RuntimeException ex) {
                if (!this.running) {
                    return;
                }
                failures++;
                String message = "Lost the insert-notification connection for queue '" + this.queue
                        + "'; new messages are picked up by the regular poll until it is back. Reconnecting in "
                        + reconnectDelay;
                if (failures == 1) {
                    logger.warn(message, ex);
                }
                else {
                    logger.warn(message + " (" + failures + " consecutive failures, latest: " + ex + ")");
                }
                if (!pause(reconnectDelay)) {
                    return;
                }
                Duration doubled = reconnectDelay.multipliedBy(2);
                reconnectDelay = doubled.compareTo(MAX_RECONNECT_DELAY) > 0 ? MAX_RECONNECT_DELAY : doubled;
            }
            finally {
                if (connection != null) {
                    release(connection);
                }
            }
        }
    }

    /**
     * Stops listening, then returns the connection to the pool.
     *
     * <p>The {@code unlisten} goes through the pool's proxy, unlike the waits for notifications,
     * which use the driver's own connection. A broken connection therefore fails here, where the
     * pool sees it - HikariCP then evicts it - instead of being lent to the next borrower.
     */
    private static void release(Connection connection) {
        try {
            execute(connection, "unlisten *");
        }
        catch (SQLException ex) {
            // Expected when the connection is broken; the pool has now seen the failure.
        }
        try {
            connection.close();
        }
        catch (SQLException ex) {
            logger.debug("Could not return the insert-notification connection to the pool", ex);
        }
    }

    private void notifyOwner() {
        try {
            this.onNotification.run();
        }
        catch (RuntimeException ex) {
            logger.warn("Handling an insert notification for queue '" + this.queue + "' failed", ex);
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static boolean pause(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
            return true;
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
