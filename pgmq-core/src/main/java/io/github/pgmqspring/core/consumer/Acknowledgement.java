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
import java.time.Instant;

/**
 * Handed to handlers running under {@link AcknowledgeMode#MANUAL} so they can decide a message's
 * fate themselves.
 *
 * <p>Exactly one of these methods should be called per message. Calling none leaves the message in
 * the queue, so it is redelivered once its visibility timeout expires - which is the safe default
 * if a handler cannot decide.
 */
public interface Acknowledgement {

    /** Deletes the message: processing succeeded. */
    void acknowledge();

    /** Moves the message to the queue's archive table. */
    void archive();

    /**
     * Makes the message visible again after {@code delay}, so it is redelivered.
     *
     * <p>Pass {@link Duration#ZERO} to redeliver immediately. This increments the message's read
     * count on the next delivery, so repeated use eventually trips poison-message protection.
     */
    void retryLater(Duration delay);

    /**
     * Makes the message visible again at a specific instant, so it is redelivered then.
     *
     * <p>The absolute counterpart to {@link #retryLater(Duration)}, for when the retry time is a
     * wall-clock decision rather than a backoff - "after the nightly batch", "when the rate-limit
     * window resets" - where computing a duration from now would drift.
     */
    void retryAt(Instant visibleAt);

    /**
     * Moves the message to the configured dead-letter queue with the given reason.
     *
     * @throws IllegalStateException if no dead-letter queue is configured
     */
    void deadLetter(String reason);

    /** Whether one of the other methods has already been called for this message. */
    boolean isAcknowledged();
}
