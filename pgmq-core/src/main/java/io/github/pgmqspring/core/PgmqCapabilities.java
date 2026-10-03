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

import org.jspecify.annotations.Nullable;

/**
 * What the PGMQ installation in the target database can actually do.
 *
 * <p>PGMQ's SQL surface changed repeatedly across 1.5 &rarr; 1.13, and not only by addition: the
 * {@code pgmq.message_record} composite type gained a {@code last_read_at} column, and functions
 * such as {@code pop} changed arity. Rather than branch on a version string everywhere - which
 * does not even exist for SQL-only installations - the client probes the catalog once at startup
 * and carries the result around.
 *
 * @param version the installed version, or {@code null} for a SQL-only install with no
 *     {@code pg_extension} row
 * @param installedAsExtension whether PGMQ is registered as a Postgres extension
 * @param messageHeaders whether {@code pgmq.message_record} has a {@code headers} column (1.5.0+)
 * @param lastReadAt whether {@code pgmq.message_record} has a {@code last_read_at} column (1.10.0+)
 * @param popWithQuantity whether {@code pgmq.pop()} accepts a quantity argument
 * @param setVisibilityTimeoutBatch whether {@code pgmq.set_vt()} accepts an array of message ids
 * @param setVisibleAt whether {@code pgmq.set_vt()} has a {@code timestamptz} overload. Without it
 *     the client derives the delay from the target instant inside the database, to whole seconds
 * @param topicRouting whether {@code pgmq.send_topic()} and the topic binding functions exist
 * @param groupedReads whether the {@code pgmq.read_grouped*} FIFO family exists
 * @param insertNotify whether {@code pgmq.enable_notify_insert()} exists
 */
public record PgmqCapabilities(
        @Nullable PgmqVersion version,
        boolean installedAsExtension,
        boolean messageHeaders,
        boolean lastReadAt,
        boolean popWithQuantity,
        boolean setVisibilityTimeoutBatch,
        boolean setVisibleAt,
        boolean topicRouting,
        boolean groupedReads,
        boolean insertNotify) {

    /** Throws if the named feature is unavailable. */
    public void require(boolean present, String feature, PgmqVersion since) {
        if (!present) {
            throw new UnsupportedPgmqFeatureException(feature, this.version, since);
        }
    }

    /**
     * Verifies that this installation is at least {@code minimumVersion}, and returns it.
     *
     * <p>A SQL-only installation has no version to compare, so it is judged by the capability that
     * defines the library's minimum instead: message headers, added in 1.5.0.
     *
     * @throws PgmqNotInstalledException if PGMQ is older than {@code minimumVersion}
     */
    public PgmqCapabilities verify(PgmqVersion minimumVersion) {
        if (this.version != null && !this.version.isAtLeast(minimumVersion)) {
            throw new PgmqNotInstalledException("PGMQ " + this.version + " is installed but this library requires "
                    + minimumVersion + " or later. Upgrade the extension with: ALTER EXTENSION pgmq UPDATE;");
        }
        if (this.version == null && !this.messageHeaders) {
            throw new PgmqNotInstalledException(
                    "The installed PGMQ does not support message headers, so it predates 1.5.0, "
                            + "which is the minimum version this library supports.");
        }
        return this;
    }

    /** A human-readable summary, used in health details and startup logging. */
    public String describe() {
        return "PGMQ " + (this.version != null ? this.version : "(unknown version, SQL-only install)")
                + (this.installedAsExtension ? " [extension]" : " [sql-only]");
    }
}
