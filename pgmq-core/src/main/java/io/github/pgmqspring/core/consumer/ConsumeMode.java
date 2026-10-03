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

/** How a listener container takes messages off the queue, and so what delivery it guarantees. */
public enum ConsumeMode {

    /**
     * {@code pgmq.read} leases a batch, and the message is acknowledged after its handler returns:
     * at-least-once, with retries counted in PGMQ's {@code read_ct}. Every message is written twice
     * - updated by the read, deleted by the acknowledgement.
     */
    READ,

    /**
     * {@code pgmq.pop} inside the handler's transaction: the message is removed exactly when the
     * handler's writes commit, and held by a row lock rather than a lease while it runs, so it is
     * never handed to another consumer meanwhile and reappears as soon as a crashed consumer's
     * connection is gone. One write per message. Requires {@code transactional}.
     */
    TRANSACTIONAL_POP,

    /**
     * {@code pgmq.pop} in its own statement before the handler runs: at-most-once. A message whose
     * handler fails, or that is in flight when the application dies, is lost. For data that can
     * be.
     */
    POP
}
