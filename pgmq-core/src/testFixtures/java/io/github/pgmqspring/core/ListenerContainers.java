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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

/**
 * Starts listener containers for a test and stops every one of them afterwards, so a failing
 * assertion never leaves a poll loop running into the next test. Register it with
 * {@code @RegisterExtension} on an instance field.
 */
public final class ListenerContainers implements AfterEachCallback {

    private final ConcurrentLinkedQueue<PgmqMessageListenerContainer<?>> containers = new ConcurrentLinkedQueue<>();

    /** Starts {@code container} and stops it when the test ends. */
    public <T> PgmqMessageListenerContainer<T> start(PgmqMessageListenerContainer<T> container) {
        track(container);
        container.start();
        return container;
    }

    /** Stops {@code container} when the test ends, for a container the test starts itself. */
    public <T> PgmqMessageListenerContainer<T> track(PgmqMessageListenerContainer<T> container) {
        this.containers.add(container);
        return container;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        PgmqMessageListenerContainer<?> container;
        while ((container = this.containers.poll()) != null) {
            container.stop();
        }
    }
}
