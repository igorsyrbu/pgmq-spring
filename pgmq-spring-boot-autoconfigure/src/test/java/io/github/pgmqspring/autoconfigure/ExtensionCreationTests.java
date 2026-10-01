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

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqNotInstalledException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Creating the extension at startup, against fresh databases that do not have PGMQ yet - which is
 * what a Postgres container started from a PGMQ image looks like until someone runs
 * {@code CREATE EXTENSION}.
 */
class ExtensionCreationTests {

    private static final JdbcTemplate admin = new JdbcTemplate(PgmqContainerSupport.dataSource());

    /** A new, empty database on the shared server, connected to as {@code user}. */
    private static DataSource freshDatabase(String prefix, String user, String password) {
        String database = PgmqContainerSupport.uniqueQueueName(prefix);
        admin.execute("create database " + database);
        PostgreSQLContainer container = PgmqContainerSupport.container();
        String url = container.getJdbcUrl().replace("/" + container.getDatabaseName(), "/" + database);
        return new DriverManagerDataSource(url, user, password);
    }

    private static boolean pgmqInstalled(DataSource dataSource) {
        return Boolean.TRUE.equals(new JdbcTemplate(dataSource).queryForObject(
                "select exists(select 1 from pg_extension where extname = 'pgmq')", Boolean.class));
    }

    private ApplicationContextRunner runner(DataSource dataSource) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgmqAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource);
    }

    @Test
    void createsTheExtensionWhenTheDatabaseDoesNotHaveIt() {
        DataSource fresh = freshDatabase("ext_create", "pgmq", "pgmq");
        assertThat(pgmqInstalled(fresh)).isFalse();

        runner(fresh).withPropertyValues("pgmq.queues[0].name=orders").run((context) -> {
            assertThat(context).hasNotFailed();
            assertThat(pgmqInstalled(fresh)).isTrue();
            assertThat(new JdbcTemplate(fresh).queryForObject(
                    "select count(*) from pgmq.list_queues() where queue_name = 'orders'", Integer.class))
                    .isEqualTo(1);
        });
        // A second start finds it installed and changes nothing.
        runner(fresh).run((context) -> assertThat(context).hasNotFailed());
    }

    @Test
    void leavesTheDatabaseAloneWhenDisabled() {
        DataSource fresh = freshDatabase("ext_disabled", "pgmq", "pgmq");

        runner(fresh).withPropertyValues("pgmq.create-extension=false").run((context) -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause().isInstanceOf(PgmqNotInstalledException.class);
        });
        assertThat(pgmqInstalled(fresh)).isFalse();
    }

    @Test
    void explainsWhyWhenTheRoleMayNotCreateIt() {
        String role = PgmqContainerSupport.uniqueQueueName("app_role");
        admin.execute("create role " + role + " login password 'app'");
        DataSource fresh = freshDatabase("ext_denied", role, "app");
        // Owned by the admin user, and PUBLIC has no CREATE on a database by default.

        runner(fresh).run((context) -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause()
                    .hasMessageContaining("permission denied");
            assertThat(context.getStartupFailure()).hasStackTraceContaining("creating it failed")
                    .hasStackTraceContaining("CREATE EXTENSION pgmq");
        });
    }
}
