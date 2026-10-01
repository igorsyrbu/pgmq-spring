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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Installs the PGMQ extension in a database that does not have it yet.
 *
 * <p>PGMQ's extension does not require a superuser: any role with the {@code CREATE} privilege on
 * the database - typically its owner - can create it. That makes installing it at startup
 * practical for development databases and containers, which ship the extension without creating it.
 */
public final class PgmqExtension {

    private static final Log logger = LogFactory.getLog(PgmqExtension.class);

    private static final String SQL_INSTALLED = "select exists(select 1 from pg_namespace where nspname = 'pgmq')";

    private PgmqExtension() {
    }

    /**
     * Creates the {@code pgmq} extension unless the {@code pgmq} schema already exists.
     *
     * <p>When PGMQ is already present - as an extension or through its SQL-only installation - no
     * statement is issued at all, so this is free in production and never needs privileges there.
     * If another instance creates the extension concurrently, that counts as success.
     *
     * @param jdbc access to the target database
     * @return {@code true} if this call created the extension
     * @throws PgmqNotInstalledException if PGMQ is missing and could not be created, with the
     *     database's reason - typically that the extension is not available on the server, or that
     *     the role lacks the {@code CREATE} privilege on the database
     */
    public static boolean createIfMissing(JdbcOperations jdbc) {
        if (installed(jdbc)) {
            return false;
        }
        try {
            jdbc.execute("create extension if not exists pgmq");
            logger.info("Created the pgmq extension, which was missing from the database");
            return true;
        }
        catch (DataAccessException ex) {
            if (installed(jdbc)) {
                return false;
            }
            throw new PgmqNotInstalledException("PGMQ is not installed in this database, and creating it failed: "
                    + ex.getMostSpecificCause().getMessage() + ". Install it once with CREATE EXTENSION pgmq; as a "
                    + "role with the CREATE privilege on the database (the extension must be available on the "
                    + "server - the ghcr.io/pgmq images ship it), or use PGMQ's SQL-only installation script.", ex);
        }
    }

    private static boolean installed(JdbcOperations jdbc) {
        return Boolean.TRUE.equals(jdbc.queryForObject(SQL_INSTALLED, Boolean.class));
    }
}
