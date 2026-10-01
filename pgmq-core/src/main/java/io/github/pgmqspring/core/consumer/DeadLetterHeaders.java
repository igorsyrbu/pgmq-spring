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
 * Header names the container adds to a message when it dead-letters it.
 *
 * <p>The original headers are preserved; these are added alongside so that an operator reading the
 * dead-letter queue can tell what failed, where it came from and how many times it was tried.
 */
public final class DeadLetterHeaders {

    /** Queue the message was originally consumed from. */
    public static final String ORIGINAL_QUEUE = "x-pgmq-original-queue";

    /** {@code msg_id} the message had in the original queue. */
    public static final String ORIGINAL_MESSAGE_ID = "x-pgmq-original-msg-id";

    /** When the message was originally enqueued. */
    public static final String ORIGINAL_ENQUEUED_AT = "x-pgmq-original-enqueued-at";

    /** PGMQ's {@code read_ct} at the time of failure: how many deliveries were attempted. */
    public static final String READ_COUNT = "x-pgmq-read-count";

    /** When the container gave up on the message. */
    public static final String FAILED_AT = "x-pgmq-failed-at";

    /** Why the message was dead-lettered. */
    public static final String REASON = "x-pgmq-failure-reason";

    /** Class name of the exception the handler threw, when there was one. */
    public static final String EXCEPTION_TYPE = "x-pgmq-exception-type";

    /** Message of the exception the handler threw, when there was one. */
    public static final String EXCEPTION_MESSAGE = "x-pgmq-exception-message";

    /** Maximum number of characters kept from an exception message. */
    public static final int MAX_EXCEPTION_MESSAGE_LENGTH = 2000;

    private DeadLetterHeaders() {
    }
}
