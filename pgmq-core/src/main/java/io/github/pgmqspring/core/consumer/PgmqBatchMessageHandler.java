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

import java.util.List;

import io.github.pgmqspring.core.PgmqMessage;

/**
 * Processes a whole batch of messages in one call.
 *
 * <p>The batch succeeds or fails as a unit: if {@link #handle} throws, the configured
 * {@link FailureAction} is applied to <em>every</em> message in the batch, including any the
 * handler had already processed internally. Handlers that need per-message outcomes should use
 * {@link PgmqMessageHandler} or {@link PgmqAcknowledgingMessageHandler} instead.
 *
 * <p>{@link AcknowledgeMode#MANUAL} is rejected for batch handlers: there is no
 * {@link Acknowledgement} to hand a batch, so nothing would ever be acknowledged.
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface PgmqBatchMessageHandler<T> {

    /**
     * Handles a batch of messages.
     *
     * @param messages the batch, never empty
     * @throws Exception to signal that the whole batch failed
     */
    void handle(List<PgmqMessage<T>> messages) throws Exception;
}
