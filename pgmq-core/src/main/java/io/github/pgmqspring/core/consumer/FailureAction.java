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

/**
 * What the container does with a message whose handler threw.
 */
public enum FailureAction {

    /**
     * Leave the message in the queue so the visibility timeout redelivers it, optionally after a
     * backoff applied with {@code pgmq.set_vt()}.
     */
    REDELIVER,

    /**
     * Move the message to a dead-letter queue, together with headers describing the failure.
     *
     * <p>The send to the dead-letter queue and the delete from the source queue run in one
     * transaction, so a message is never both dead-lettered and redelivered, nor lost between the
     * two.
     */
    DEAD_LETTER,

    /** Move the message to the queue's archive table. */
    ARCHIVE
}
