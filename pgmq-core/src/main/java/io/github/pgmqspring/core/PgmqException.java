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
 * Base class for every exception thrown by pgmq-spring.
 *
 * <p>All failures originating in this library are reported as a subclass of this type, so
 * applications can catch {@code PgmqException} to handle "something went wrong talking to PGMQ"
 * without catching unrelated {@link org.springframework.dao.DataAccessException}s.
 */
public class PgmqException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PgmqException(String message) {
        super(message);
    }

    public PgmqException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
