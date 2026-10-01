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

/**
 * The kind of physical queue to create.
 */
public enum QueueKind {

    /** A regular, WAL-logged queue. Created with {@code pgmq.create()}. */
    STANDARD,

    /**
     * An unlogged queue, created with {@code pgmq.create_unlogged()}.
     *
     * <p>Writes skip the write-ahead log, which is considerably faster, but the queue's contents
     * are <strong>truncated on crash recovery</strong> and are not replicated to standbys. Use
     * only for messages that can be lost.
     */
    UNLOGGED,

    /**
     * A partitioned queue, created with {@code pgmq.create_partitioned()}.
     *
     * <p>Requires the {@code pg_partman} extension to be installed in the database.
     */
    PARTITIONED
}
