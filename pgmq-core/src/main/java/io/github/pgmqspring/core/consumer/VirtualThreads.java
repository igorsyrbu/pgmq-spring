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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates virtual-thread factories on JDK 21+ while still compiling against the JDK 17 baseline.
 *
 * <p>The library targets Java 17 so that it can be consumed on every JDK Spring Boot supports, but
 * a polling consumer is exactly the workload virtual threads are for: mostly blocked on the
 * database. Rather than ship a multi-release jar, this class looks up {@code Thread.ofVirtual()}
 * reflectively once and falls back to platform threads when it is absent.
 */
final class VirtualThreads {

    private static final MethodHandle OF_VIRTUAL;

    private static final MethodHandle BUILDER_NAME;

    private static final MethodHandle BUILDER_FACTORY;

    static {
        MethodHandle ofVirtual = null;
        MethodHandle name = null;
        MethodHandle factory = null;
        try {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            Class<?> builderClass = Class.forName("java.lang.Thread$Builder$OfVirtual");
            Class<?> baseBuilder = Class.forName("java.lang.Thread$Builder");
            ofVirtual = lookup.findStatic(Thread.class, "ofVirtual", MethodType.methodType(builderClass));
            name = lookup.findVirtual(baseBuilder, "name",
                    MethodType.methodType(baseBuilder, String.class, long.class));
            factory = lookup.findVirtual(baseBuilder, "factory", MethodType.methodType(ThreadFactory.class));
        }
        catch (ReflectiveOperationException | RuntimeException ex) {
            // JDK 17: virtual threads are unavailable, which is fine.
            ofVirtual = null;
            name = null;
            factory = null;
        }
        OF_VIRTUAL = ofVirtual;
        BUILDER_NAME = name;
        BUILDER_FACTORY = factory;
    }

    private VirtualThreads() {
    }

    /** Whether the running JDK supports virtual threads. */
    static boolean available() {
        return OF_VIRTUAL != null;
    }

    /**
     * Returns a virtual-thread factory when the JDK supports one, and a daemon platform-thread
     * factory otherwise.
     *
     * @param namePrefix prefix for thread names
     */
    static ThreadFactory factory(String namePrefix) {
        if (available()) {
            try {
                Object builder = OF_VIRTUAL.invoke();
                builder = BUILDER_NAME.invoke(builder, namePrefix, 0L);
                return (ThreadFactory) BUILDER_FACTORY.invoke(builder);
            }
            catch (Throwable ex) {
                // Fall through to platform threads.
            }
        }
        AtomicLong counter = new AtomicLong();
        return (runnable) -> {
            Thread thread = new Thread(runnable, namePrefix + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}
