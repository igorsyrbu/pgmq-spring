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
 * What the container does with a message after the handler returns normally.
 */
public enum AcknowledgeMode {

    /** Delete the message. The default, and what most applications want. */
    DELETE,

    /**
     * Move the message to the queue's archive table, keeping it for audit or replay.
     *
     * <p>The archive grows without bound; prune it yourself, or use a partitioned queue with a
     * retention interval.
     */
    ARCHIVE,

    /**
     * Do nothing: the handler decides, via the {@link Acknowledgement} passed to it.
     *
     * <p>A handler that returns without calling any {@code Acknowledgement} method leaves the
     * message untouched, so it is redelivered when its visibility timeout expires.
     */
    MANUAL
}
