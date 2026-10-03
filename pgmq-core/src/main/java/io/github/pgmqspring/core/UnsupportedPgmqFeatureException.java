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
 * Thrown when an operation requires a PGMQ feature that the installed version does not provide -
 * for example topic routing or grouped (FIFO) reads against PGMQ&nbsp;1.5.
 *
 * @see PgmqCapabilities
 */
public class UnsupportedPgmqFeatureException extends PgmqException {

    private static final long serialVersionUID = 1L;

    /**
     * @param installed the installed version, or {@code null} for a SQL-only installation, which
     *     records none
     */
    public UnsupportedPgmqFeatureException(String feature, @Nullable PgmqVersion installed, PgmqVersion required) {
        super("PGMQ feature '" + feature + "' requires PGMQ " + required + " or later, but the database has "
                + (installed != null ? installed : "a SQL-only installation without that feature"));
    }

    public UnsupportedPgmqFeatureException(String message) {
        super(message);
    }
}
