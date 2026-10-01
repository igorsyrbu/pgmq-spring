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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.github.pgmqspring.core.PgmqMessage;

/**
 * Fans every callback out to several {@link ConsumerListener}s.
 *
 * <p>Having more than one listener is the normal case, not the exception: the Micrometer
 * instrumentation is a listener, and an application that wants to react to dead-lettered messages
 * adds another. Treating them as mutually exclusive would force users to choose between metrics
 * and their own bookkeeping.
 *
 * <p>A delegate that throws is logged and skipped, so one misbehaving listener cannot stop the
 * others from running or affect message handling.
 */
public class CompositeConsumerListener implements ConsumerListener {

    private static final Log logger = LogFactory.getLog(CompositeConsumerListener.class);

    private final List<ConsumerListener> delegates;

    public CompositeConsumerListener(List<? extends ConsumerListener> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    /**
     * Returns a listener for the given delegates: the single delegate when there is exactly one,
     * a no-op when there are none, and a composite otherwise.
     */
    public static ConsumerListener of(List<? extends ConsumerListener> delegates) {
        if (delegates.isEmpty()) {
            return new ConsumerListener() {
            };
        }
        if (delegates.size() == 1) {
            return delegates.get(0);
        }
        return new CompositeConsumerListener(delegates);
    }

    /** The delegates, in invocation order. */
    public List<ConsumerListener> getDelegates() {
        return this.delegates;
    }

    @Override
    public void onPolled(String queue, List<? extends PgmqMessage<?>> messages) {
        each((delegate) -> delegate.onPolled(queue, messages));
    }

    @Override
    public void onSuccess(String queue, PgmqMessage<?> message, Duration duration) {
        each((delegate) -> delegate.onSuccess(queue, message, duration));
    }

    @Override
    public void onFailure(String queue, PgmqMessage<?> message, Duration duration, Throwable error) {
        each((delegate) -> delegate.onFailure(queue, message, duration, error));
    }

    @Override
    public void onPoison(String queue, PgmqMessage<?> message) {
        each((delegate) -> delegate.onPoison(queue, message));
    }

    @Override
    public void onDeadLettered(String queue, PgmqMessage<?> message, String deadLetterQueue, String reason) {
        each((delegate) -> delegate.onDeadLettered(queue, message, deadLetterQueue, reason));
    }

    @Override
    public void onPollError(String queue, Throwable error) {
        each((delegate) -> delegate.onPollError(queue, error));
    }

    private void each(java.util.function.Consumer<ConsumerListener> action) {
        for (ConsumerListener delegate : this.delegates) {
            try {
                action.accept(delegate);
            }
            catch (Throwable ex) {
                logger.warn("ConsumerListener " + delegate.getClass().getName() + " threw; continuing with the "
                        + "remaining listeners", ex);
            }
        }
    }
}
