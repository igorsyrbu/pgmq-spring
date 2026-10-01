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

package io.github.pgmqspring.core.client;

/**
 * PGMQ's FIFO group (grouped read) support, available from PGMQ 1.10.0.
 *
 * <p>A grouped read guarantees that for any group key, <strong>at most one message is in flight at
 * a time</strong>: while an earlier message for a key is unacknowledged, PGMQ withholds the whole
 * group from every reader, not just the one that claimed it. Two consumers polling concurrently -
 * in the same JVM or on different machines - can therefore never both hold an unacknowledged
 * message for the same key, and a key's messages are delivered in send order.
 *
 * <p>That is what makes ordered, effectively single-consumer-at-a-time processing per key possible
 * on what is otherwise a competing-consumer queue. It is the same model SQS FIFO queues use with
 * their message group id.
 *
 * <p><strong>It is not a partition assignment.</strong> No consumer instance owns a key the way a
 * Kafka consumer owns a partition; once a message is acknowledged, the next message for that key
 * goes to whichever consumer polls first. The guarantee is "never concurrent, always in order",
 * not "always the same instance".
 *
 * <p>The key travels as an ordinary PGMQ header. {@value #GROUP_HEADER} is the exact header name
 * PGMQ's grouped-read functions look at - verified against the PGMQ 1.13.0 SQL source, which reads
 * {@code COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')} - so using this constant keeps
 * messages interoperable with other PGMQ clients and with plain SQL.
 *
 * @see SendOptions#group(String)
 * @see GroupReadStrategy
 * @see PgmqOperations#readGrouped(String, ReadOptions)
 */
public final class FifoGroups {

    /** The header PGMQ groups messages by. */
    public static final String GROUP_HEADER = "x-pgmq-group";

    /** The group PGMQ treats a message as belonging to when it carries no {@value #GROUP_HEADER}. */
    public static final String DEFAULT_GROUP = "_default_fifo_group";

    private FifoGroups() {
    }
}
