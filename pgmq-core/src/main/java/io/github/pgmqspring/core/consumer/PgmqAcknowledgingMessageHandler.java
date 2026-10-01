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
 * Processes one message and decides its fate itself.
 *
 * <p>Used with {@link AcknowledgeMode#MANUAL}. With any other acknowledge mode the container
 * acknowledges on the handler's behalf and the {@link Acknowledgement} is still supplied, but
 * calling it is then redundant.
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface PgmqAcknowledgingMessageHandler<T> {

    /**
     * Handles one message.
     *
     * @param message the message
     * @param acknowledgement decides whether the message is deleted, archived, retried or
     *     dead-lettered
     * @throws Exception to signal failure; the container applies the configured failure action
     */
    void handle(PgmqMessage<T> message, Acknowledgement acknowledgement) throws Exception;
}
