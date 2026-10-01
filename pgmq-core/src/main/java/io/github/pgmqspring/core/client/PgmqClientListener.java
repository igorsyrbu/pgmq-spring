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

package io.github.pgmqspring.core.client;

import java.time.Duration;
import java.util.List;

/**
 * Producer-side callbacks for {@link PgmqTemplate}.
 *
 * <p>The mirror image of {@code ConsumerListener}: it lets metrics and tracing observe sends
 * without the core client depending on Micrometer. Callbacks run on the calling thread, so they
 * must not block. Exceptions thrown by a callback are swallowed and logged.
 */
public interface PgmqClientListener {

    /** Messages were successfully written to {@code queue}. */
    default void onSent(String queue, List<Long> messageIds, Duration duration) {
    }
}
