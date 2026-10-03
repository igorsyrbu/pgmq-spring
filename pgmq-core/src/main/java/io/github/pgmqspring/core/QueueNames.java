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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Client-side validation of PGMQ queue names, mirroring the rules PGMQ itself enforces.
 *
 * <p>The rules are taken from the PGMQ source rather than from prose documentation:
 *
 * <ul>
 *   <li>{@code pgmq.validate_queue_name()} rejects names longer than {@value #MAX_LENGTH}
 *       characters. The limit exists because PGMQ builds table identifiers such as
 *       {@code template_pgmq_q_<queue>}; the 16-character prefix plus 47 characters is exactly
 *       Postgres's 63-byte identifier limit.
 *   <li>{@code pgmq.format_table_name()} rejects names containing {@code $}, {@code ;},
 *       {@code --} or a single quote.
 * </ul>
 *
 * <p><strong>Case folding.</strong> {@code format_table_name()} lower-cases the queue name when it
 * builds the backing table name, so {@code Orders} and {@code orders} resolve to the <em>same</em>
 * physical queue even though {@code pgmq.list_queues()} reports whichever spelling was used at
 * creation time. This class does not reject mixed-case names - PGMQ accepts them - but
 * {@link #normalize(String)} exposes the folded form so callers can detect collisions.
 */
public final class QueueNames {

    /** Maximum queue-name length accepted by PGMQ. */
    public static final int MAX_LENGTH = 47;

    private static final Pattern FORBIDDEN = Pattern.compile("[$;']|--");

    private QueueNames() {
    }

    /**
     * Validates a queue name and returns it unchanged.
     *
     * @param queueName the name to validate
     * @return the same name, for fluent use
     * @throws InvalidQueueNameException if the name is null, empty, too long, or contains a
     *     character PGMQ forbids
     */
    public static String validate(String queueName) {
        if (queueName == null || queueName.isEmpty()) {
            throw new InvalidQueueNameException(String.valueOf(queueName), "must not be null or empty");
        }
        if (queueName.length() > MAX_LENGTH) {
            throw new InvalidQueueNameException(
                    queueName,
                    "maximum length is " + MAX_LENGTH + " characters but was " + queueName.length());
        }
        if (FORBIDDEN.matcher(queueName).find()) {
            throw new InvalidQueueNameException(
                    queueName, "must not contain '$', ';', '--' or a single quote");
        }
        if (queueName.isBlank()) {
            throw new InvalidQueueNameException(queueName, "must not be blank");
        }
        return queueName;
    }

    /**
     * Returns the effective (lower-cased) queue name that PGMQ will use to derive table names.
     *
     * <p>Two names with the same normalized form address the same queue.
     */
    public static String normalize(String queueName) {
        return queueName.toLowerCase(Locale.ROOT);
    }
}
