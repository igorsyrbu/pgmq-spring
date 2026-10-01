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
 * Thrown when a queue name violates PGMQ's naming rules.
 *
 * <p>Validation happens client-side, before any SQL is issued, so that an invalid name produces a
 * clear error instead of a Postgres syntax or identifier-length error.
 *
 * @see QueueNames
 */
public class InvalidQueueNameException extends PgmqException {

    private static final long serialVersionUID = 1L;

    private final String queueName;

    public InvalidQueueNameException(String queueName, String reason) {
        super("Invalid PGMQ queue name '" + queueName + "': " + reason);
        this.queueName = queueName;
    }

    /** The rejected queue name. */
    public String getQueueName() {
        return this.queueName;
    }
}
