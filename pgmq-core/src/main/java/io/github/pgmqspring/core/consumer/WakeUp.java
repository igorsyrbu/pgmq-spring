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

/** How a listener container's polling loops learn that new messages may be waiting. */
public enum WakeUp {

    /** Poll, backing off from {@code pollDelay} to {@code maxPollDelay} while the queue is empty. */
    POLL,

    /**
     * Also wake up when PGMQ notifies that a message was inserted, so new messages are picked up
     * at once and the regular poll becomes a slow fallback. Requires PGMQ 1.10.0, insert
     * notifications enabled on the queue, and the PostgreSQL JDBC driver.
     */
    NOTIFY
}
