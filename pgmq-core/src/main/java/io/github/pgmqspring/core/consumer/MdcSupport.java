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

import org.jspecify.annotations.Nullable;

/**
 * Minimal, optional access to SLF4J's {@code MDC}.
 *
 * <p>The core module logs through commons-logging (which Spring's {@code spring-jcl} routes to
 * whatever the application actually uses), so SLF4J is not a required dependency. MDC has no
 * commons-logging equivalent, so it is bound reflectively once at class-initialization time and
 * degrades to a no-op when SLF4J is absent. Method handles keep the per-message cost negligible.
 */
final class MdcSupport {

    private static final @Nullable MethodHandle PUT;

    private static final @Nullable MethodHandle GET;

    private static final @Nullable MethodHandle REMOVE;

    static {
        MethodHandle put = null;
        MethodHandle get = null;
        MethodHandle remove = null;
        try {
            Class<?> mdc = Class.forName("org.slf4j.MDC");
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            put = lookup.findStatic(mdc, "put", MethodType.methodType(void.class, String.class, String.class));
            get = lookup.findStatic(mdc, "get", MethodType.methodType(String.class, String.class));
            remove = lookup.findStatic(mdc, "remove", MethodType.methodType(void.class, String.class));
        }
        catch (ReflectiveOperationException | RuntimeException ex) {
            put = null;
            get = null;
            remove = null;
        }
        PUT = put;
        GET = get;
        REMOVE = remove;
    }

    private MdcSupport() {
    }

    static void put(String key, String value) {
        if (PUT == null) {
            return;
        }
        try {
            PUT.invokeExact(key, value);
        }
        catch (Throwable ex) {
            // Logging context is best-effort; never let it affect message handling.
        }
    }

    static @Nullable String get(String key) {
        if (GET == null) {
            return null;
        }
        try {
            return (String) GET.invokeExact(key);
        }
        catch (Throwable ex) {
            return null;
        }
    }

    static void remove(String key) {
        if (REMOVE == null) {
            return;
        }
        try {
            REMOVE.invokeExact(key);
        }
        catch (Throwable ex) {
            // Best-effort.
        }
    }
}
