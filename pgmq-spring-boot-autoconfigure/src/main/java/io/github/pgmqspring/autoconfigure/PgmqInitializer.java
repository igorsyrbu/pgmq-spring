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
import java.util.List;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqExtension;
import io.github.pgmqspring.core.PgmqVersion;
import io.github.pgmqspring.core.QueueKind;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Runs PGMQ's startup checks: verify the extension is present and new enough, then create any
 * queues listed under {@code pgmq.queues}, with the FIFO index and insert notifications they ask for.
 *
 * <p>This runs as an {@link InitializingBean} rather than an {@code ApplicationRunner} so that a
 * missing or too-old PGMQ fails context startup, before the application begins serving traffic.
 */
public class PgmqInitializer implements InitializingBean {

    private static final Log logger = LogFactory.getLog(PgmqInitializer.class);

    private final PgmqProperties properties;

    private final ObjectProvider<PgmqOperations> pgmq;

    private final DataSource dataSource;

    public PgmqInitializer(PgmqProperties properties, ObjectProvider<PgmqOperations> pgmq, DataSource dataSource) {
        this.properties = properties;
        this.pgmq = pgmq;
        this.dataSource = dataSource;
    }

    @Override
    public void afterPropertiesSet() {
        PgmqOperations template = this.pgmq.getIfUnique();
        if (template == null) {
            logger.debug("Skipping PGMQ startup checks: there is not exactly one PgmqOperations bean");
            return;
        }
        if (this.properties.isCreateExtension()) {
            PgmqExtension.createIfMissing(new JdbcTemplate(this.dataSource));
        }
        if (this.properties.isVerifyOnStartup()) {
            PgmqVersion minimum = PgmqVersion.parse(this.properties.getMinimumVersion());
            if (minimum == null) {
                throw new IllegalStateException(
                        "pgmq.minimum-version is not a valid version: " + this.properties.getMinimumVersion());
            }
            PgmqCapabilities capabilities = template.capabilities().verify(minimum);
            logger.info("Verified " + capabilities.describe());
        }
        List<PgmqProperties.Queue> queues = this.properties.getQueues();
        for (int i = 0; i < queues.size(); i++) {
            PgmqProperties.Queue queue = queues.get(i);
            String name = queue.getName();
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("pgmq.queues[" + i + "].name is required");
            }
            QueueNames.validate(name);
            ensureQueue(template, queue, name);
            if (queue.isFifoIndex()) {
                // create_fifo_index uses IF NOT EXISTS, so after losing a race a second call succeeds;
                // a genuine failure (PGMQ older than 1.10.0) fails again and is reported.
                ensure(name, () -> template.createFifoIndex(name), () -> {
                    template.createFifoIndex(name);
                    return true;
                });
            }
            if (queue.isNotifyOnInsert()) {
                ensureNotifyOnInsert(template, queue, name);
            }
            logger.info("Ensured PGMQ queue '" + name + "' (" + queue.getKind() + ")"
                    + (queue.isFifoIndex() ? " with a FIFO index" : "")
                    + (queue.isNotifyOnInsert() ? " with insert notifications" : "") + " exists");
        }
    }

    /**
     * Enables insert notifications only when they are off or their throttle differs: enabling
     * recreates the queue's trigger, which briefly locks the table against inserts, so doing it on
     * every start of every instance would stall producers for nothing.
     */
    private static void ensureNotifyOnInsert(PgmqOperations template, PgmqProperties.Queue queue, String name) {
        Duration throttle = queue.getNotifyThrottle();
        Duration current = template.notifyInsertThrottle(name);
        if (current != null && (throttle == null || current.equals(throttle))) {
            return;
        }
        ensure(name, () -> {
            if (throttle != null) {
                template.enableNotifyInsert(name, throttle);
            }
            else {
                template.enableNotifyInsert(name);
            }
        }, () -> template.notifyInsertThrottle(name) != null);
    }

    private static void ensureQueue(PgmqOperations template, PgmqProperties.Queue queue, String name) {
        if (template.queueExists(name)) {
            return;
        }
        ensure(name, () -> {
            if (queue.getKind() == QueueKind.PARTITIONED) {
                template.createPartitionedQueue(name, queue.getPartitionInterval(), queue.getRetentionInterval());
            }
            else {
                template.createQueue(name, queue.getKind());
            }
        }, () -> template.queueExists(name));
    }

    /**
     * Runs an idempotent DDL call that can still fail under concurrency.
     *
     * <p>PGMQ creates queues with {@code IF NOT EXISTS}, which Postgres does not guarantee to be safe
     * against a concurrent creator. A failure is therefore re-checked, and ignored when the object
     * now exists, so that instances starting together cannot fail on it.
     */
    private static void ensure(String name, Runnable create, BooleanSupplier recovered) {
        try {
            create.run();
        }
        catch (RuntimeException ex) {
            boolean ok;
            try {
                ok = recovered.getAsBoolean();
            }
            catch (RuntimeException again) {
                ex.addSuppressed(again);
                throw ex;
            }
            if (!ok) {
                throw ex;
            }
            logger.debug("PGMQ queue '" + name + "' was set up concurrently by another instance", ex);
        }
    }
}
