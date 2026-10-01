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

package io.github.pgmqspring.core;

import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Postgres+PGMQ container for integration tests.
 *
 * <p>One container is started per JVM and reused by every test class, which keeps the suite fast
 * and avoids Docker churn. Tests isolate themselves with unique queue names rather than by
 * restarting the database.
 *
 * <p>The image is selected by the {@code pgmq.image} system property so the same suite can run
 * against the minimum supported PGMQ and the latest release. Tags are always pinned - never
 * {@code latest} - so a test run is reproducible.
 */
public final class PgmqContainerSupport {

    /**
     * Default image: the newest supported PGMQ release. Run the suite against the oldest,
     * {@code v1.5.1}, as well.
     */
    public static final String DEFAULT_IMAGE = "ghcr.io/pgmq/pg17-pgmq:v1.13.0";

    private static final AtomicInteger QUEUE_SEQUENCE = new AtomicInteger();

    private static volatile PostgreSQLContainer container;

    private static volatile HikariDataSource dataSource;

    private PgmqContainerSupport() {
    }

    /** The image under test, overridable with {@code -Dpgmq.image=...}. */
    public static String image() {
        return System.getProperty("pgmq.image", DEFAULT_IMAGE);
    }

    /** Starts the shared container on first call and returns it. */
    public static synchronized PostgreSQLContainer container() {
        if (container == null) {
            PostgreSQLContainer started = new PostgreSQLContainer(
                    DockerImageName.parse(image()).asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("pgmq_test")
                    .withUsername("pgmq")
                    .withPassword("pgmq");
            started.start();
            container = started;
            Runtime.getRuntime().addShutdownHook(new Thread(started::stop));
        }
        return container;
    }

    /** A pooled {@link DataSource} against the shared container, with PGMQ already installed. */
    public static synchronized DataSource dataSource() {
        if (dataSource == null) {
            PostgreSQLContainer running = container();
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(running.getJdbcUrl());
            config.setUsername(running.getUsername());
            config.setPassword(running.getPassword());
            // Long-polling tests hold a connection for the whole poll window, so the pool must be
            // comfortably larger than the number of concurrent consumers under test.
            config.setMaximumPoolSize(16);
            config.setPoolName("pgmq-test");
            HikariDataSource created = new HikariDataSource(config);
            new JdbcTemplate(created).execute("create extension if not exists pgmq");
            dataSource = created;
        }
        return dataSource;
    }

    /** A queue name unique to this JVM run, so tests never collide. */
    public static String uniqueQueueName(String prefix) {
        return QueueNames.validate(prefix + "_" + QUEUE_SEQUENCE.incrementAndGet());
    }
}
