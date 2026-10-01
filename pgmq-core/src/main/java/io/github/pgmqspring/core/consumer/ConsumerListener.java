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

import java.time.Duration;
import java.util.List;

import io.github.pgmqspring.core.PgmqMessage;

/**
 * Lifecycle callbacks for a {@link PgmqMessageListenerContainer}.
 *
 * <p>This is the seam the observability layer plugs into: it lets metrics, tracing and logging be
 * added without the core runtime depending on Micrometer. Every method has a no-op default, so
 * implementations override only what they care about.
 *
 * <p>Callbacks run on the consumer thread and must not block, or they will slow the consumer down.
 * Exceptions thrown by a callback are caught and logged; they never affect message handling.
 */
public interface ConsumerListener {

    /** A poll returned {@code messages}, possibly empty. */
    default void onPolled(String queue, List<? extends PgmqMessage<?>> messages) {
    }

    /** A handler returned normally and the message was acknowledged. */
    default void onSuccess(String queue, PgmqMessage<?> message, Duration duration) {
    }

    /** A handler threw. The configured {@link FailureAction} is applied next. */
    default void onFailure(String queue, PgmqMessage<?> message, Duration duration, Throwable error) {
    }

    /** A message exceeded {@code maxAttempts} and was treated as poison without being handled. */
    default void onPoison(String queue, PgmqMessage<?> message) {
    }

    /** A message was moved to the dead-letter queue. */
    default void onDeadLettered(String queue, PgmqMessage<?> message, String deadLetterQueue, String reason) {
    }

    /** The container's poll loop itself failed, rather than a handler. */
    default void onPollError(String queue, Throwable error) {
    }
}
