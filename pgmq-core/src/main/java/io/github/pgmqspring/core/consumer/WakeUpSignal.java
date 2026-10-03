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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * What a container's polling loops wait on between polls: a timeout that a notification, a
 * resume or a stop can cut short.
 *
 * <p>A waiter passes the generation it saw before its poll, so a signal that arrives while it is
 * still polling is not lost: the wait then returns at once. A lock rather than a monitor, so a
 * virtual thread waiting here parks instead of pinning its carrier.
 */
final class WakeUpSignal {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition signalled = this.lock.newCondition();

    private long generation;

    long generation() {
        this.lock.lock();
        try {
            return this.generation;
        }
        finally {
            this.lock.unlock();
        }
    }

    void signal() {
        this.lock.lock();
        try {
            this.generation++;
            this.signalled.signalAll();
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * Waits up to {@code timeout}, or until signalled after {@code seenGeneration}.
     *
     * @return whether a signal ended the wait
     */
    boolean await(Duration timeout, long seenGeneration) {
        this.lock.lock();
        try {
            long remaining = timeout.toNanos();
            while (this.generation == seenGeneration) {
                if (remaining <= 0) {
                    return false;
                }
                remaining = this.signalled.awaitNanos(remaining);
            }
            return true;
        }
        catch (InterruptedException ex) {
            // Restore the flag: the poll loop checks it and exits.
            Thread.currentThread().interrupt();
            return false;
        }
        finally {
            this.lock.unlock();
        }
    }
}
