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

import org.jspecify.annotations.Nullable;

/**
 * Thrown by startup verification when the {@code pgmq} schema cannot be found in the target
 * database, or when the installed PGMQ version is older than the minimum this library supports.
 */
public class PgmqNotInstalledException extends PgmqException {

    private static final long serialVersionUID = 1L;

    public PgmqNotInstalledException(String message) {
        super(message);
    }

    public PgmqNotInstalledException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
