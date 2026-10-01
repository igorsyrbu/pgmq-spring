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

package io.github.pgmqspring.samples.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import io.github.pgmqspring.core.PgmqMessage;

/**
 * The consuming side: a whole batch per call, written with one multi-row insert.
 *
 * <p>Two rules make a batch handler safe:
 *
 * <ul>
 *   <li><strong>Be idempotent.</strong> Delivery is at-least-once, and a batch is redelivered
 *       <em>as a whole</em>, so every message in it may be seen again. {@code on conflict do
 *       nothing} on the reading id makes a repeat harmless.
 *   <li><strong>Throw only for problems a retry can fix.</strong> A throw fails every message in
 *       the batch - they are all retried, and after {@code maxAttempts} all dead-lettered, good
 *       ones included. So a reading that is simply invalid is recorded as rejected instead, and
 *       its batch-mates still succeed. Throwing is reserved for transient failures, such as the
 *       database being briefly unavailable.
 * </ul>
 */
@Component
public class ReadingBatchHandler {

    /** A device whose readings fail on their first delivery, to show a whole batch being retried. */
    public static final String FLAKY_DEVICE = "flaky-sensor";

    /** Lowest plausible reading; anything outside the range is rejected. */
    public static final double MIN_VALUE = -50;

    /** Highest plausible reading. */
    public static final double MAX_VALUE = 150;

    private static final Log logger = LogFactory.getLog(ReadingBatchHandler.class);

    private final JdbcTemplate jdbc;

    private final AtomicInteger largestBatch = new AtomicInteger();

    public ReadingBatchHandler(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Handles one batch. Throwing retries the whole batch. */
    public void handle(List<PgmqMessage<Reading>> batch) {
        this.largestBatch.accumulateAndGet(batch.size(), Math::max);
        logger.info("Handling a batch of " + batch.size() + " readings");

        for (PgmqMessage<Reading> message : batch) {
            if (FLAKY_DEVICE.equals(message.payload().device()) && message.readCount() == 1) {
                // Stands in for a transient failure. The whole batch rolls back and is redelivered.
                throw new IllegalStateException("device " + FLAKY_DEVICE + " is warming up; retry the batch");
            }
        }

        List<Object[]> valid = new ArrayList<>();
        List<Object[]> rejected = new ArrayList<>();
        for (PgmqMessage<Reading> message : batch) {
            Reading reading = message.payload();
            if (reading.value() >= MIN_VALUE && reading.value() <= MAX_VALUE) {
                valid.add(new Object[] {reading.id(), reading.device(), reading.value()});
            }
            else {
                rejected.add(new Object[] {reading.id(), "value " + reading.value() + " is out of range"});
            }
        }
        this.jdbc.batchUpdate("insert into readings(id, device, value) values (?, ?, ?) "
                + "on conflict (id) do nothing", valid);
        this.jdbc.batchUpdate("insert into rejected_readings(id, reason) values (?, ?) "
                + "on conflict (id) do nothing", rejected);
    }

    /** The largest batch handled so far, to show that batching actually happens. */
    public int largestBatch() {
        return this.largestBatch.get();
    }
}
