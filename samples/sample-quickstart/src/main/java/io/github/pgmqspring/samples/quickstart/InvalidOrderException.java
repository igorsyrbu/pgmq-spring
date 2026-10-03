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

package io.github.pgmqspring.samples.quickstart;

/**
 * An order that can never be processed, however often it is retried. The container is configured
 * to treat it as non-retryable, so such an order is dead-lettered on its first failure.
 */
public class InvalidOrderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidOrderException(String message) {
        super(message);
    }
}
