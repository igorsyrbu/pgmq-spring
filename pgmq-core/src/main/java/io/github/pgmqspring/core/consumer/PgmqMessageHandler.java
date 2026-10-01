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

import io.github.pgmqspring.core.PgmqMessage;

/**
 * Processes one message at a time.
 *
 * <p>Throwing from {@link #handle} tells the container the message failed, which triggers the
 * configured {@link FailureAction}. Returning normally triggers the configured
 * {@link AcknowledgeMode}.
 *
 * <p>Handlers must be <strong>idempotent</strong>: PGMQ gives at-least-once delivery, so a handler
 * that completes its work and then crashes before acknowledging will see the same message again.
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface PgmqMessageHandler<T> {

    /**
     * Handles one message.
     *
     * @param message the message, including its read count and headers
     * @throws Exception to signal failure; the container applies the configured failure action
     */
    void handle(PgmqMessage<T> message) throws Exception;
}
