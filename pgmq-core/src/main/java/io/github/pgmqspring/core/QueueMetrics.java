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

package io.github.pgmqspring.core;

import java.time.OffsetDateTime;

import org.jspecify.annotations.Nullable;

/**
 * A snapshot of {@code pgmq.metrics()} for one queue.
 *
 * @param queueName the queue
 * @param queueLength total messages currently in the queue table, visible or not
 * @param newestMessageAgeSeconds age of the newest message, or {@code null} when the queue is empty
 * @param oldestMessageAgeSeconds age of the oldest message, or {@code null} when the queue is empty
 * @param totalMessages messages ever enqueued (the queue's sequence high-water mark)
 * @param scrapeTime when PGMQ produced this snapshot
 * @param visibleLength messages currently visible to a reader
 * @param defaultPartitionLength rows in the default partition; {@code null} on PGMQ versions that do
 *     not report this column (absent in 1.6.1, present in 1.13.0)
 */
public record QueueMetrics(
        String queueName,
        long queueLength,
        @Nullable Integer newestMessageAgeSeconds,
        @Nullable Integer oldestMessageAgeSeconds,
        long totalMessages,
        OffsetDateTime scrapeTime,
        long visibleLength,
        @Nullable Long defaultPartitionLength) {
}
