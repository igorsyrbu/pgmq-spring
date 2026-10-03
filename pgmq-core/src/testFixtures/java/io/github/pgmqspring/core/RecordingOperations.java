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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * Wraps a {@link PgmqOperations} to record every call made through it, and optionally to fail
 * chosen calls, so a test can assert which statements a consumer issued.
 *
 * <p>A call is described by its method name, followed by {@code [n]} when its second argument is a
 * collection of {@code n} ids: {@code delete}, {@code delete[10]}.
 */
public final class RecordingOperations {

    private final PgmqOperations delegate;

    private final List<String> calls = new CopyOnWriteArrayList<>();

    private final AtomicInteger remainingFailures = new AtomicInteger();

    private volatile Predicate<String> failingCalls = (call) -> false;

    public RecordingOperations(PgmqOperations delegate) {
        this.delegate = delegate;
    }

    /** Fails the next {@code times} calls whose description matches {@code call}, before they reach the delegate. */
    public void failNext(int times, Predicate<String> call) {
        this.failingCalls = call;
        this.remainingFailures.set(times);
    }

    /** The method name of every call, in order. */
    public List<String> calls() {
        return this.calls.stream().map((call) -> call.replaceAll("\\[.*", "")).toList();
    }

    /** Delete and archive calls, as {@code delete[n]} for n ids or {@code delete} for one. */
    public List<String> acknowledgements() {
        return this.calls.stream().filter((call) -> call.startsWith("delete") || call.startsWith("archive"))
                .toList();
    }

    public PgmqOperations proxy() {
        return (PgmqOperations) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {PgmqOperations.class}, (proxy, method, args) -> {
                    String name = method.getName();
                    boolean collection = args != null && args.length > 1 && args[1] instanceof Collection<?>;
                    String call = collection ? name + "[" + ((Collection<?>) args[1]).size() + "]" : name;
                    this.calls.add(call);
                    if (this.failingCalls.test(call)
                            && this.remainingFailures.getAndUpdate((n) -> Math.max(0, n - 1)) > 0) {
                        throw new PgmqException("simulated failure of " + call);
                    }
                    try {
                        return method.invoke(this.delegate, args);
                    }
                    catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }
}
