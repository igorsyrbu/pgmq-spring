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

package io.github.pgmqspring.autoconfigure;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.util.ClassUtils;

import io.github.pgmqspring.core.convert.Jackson2PayloadConverter;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;
import io.github.pgmqspring.core.convert.PayloadConverter;

/**
 * Chooses a {@link PayloadConverter} without the library committing to a Jackson generation.
 *
 * <p>Spring Boot 4 defaults to Jackson 3 and deprecates its Jackson 2 auto-configuration for
 * removal in 4.3, but applications can still be running either. The rule is:
 *
 * <ol>
 *   <li>The application's Jackson 3 ({@code tools.jackson}) mapper bean, if it has one.
 *   <li>Otherwise the application's Jackson 2 ({@code com.fasterxml.jackson}) mapper bean - an
 *       application still configured for Jackson 2 keeps its settings even when Jackson 3 is also
 *       on the classpath, as it often is transitively.
 *   <li>Otherwise a default mapper, Jackson 3 if present, else Jackson 2.
 *   <li>Otherwise fail with a message naming what to add.
 * </ol>
 *
 * <p>Reusing the application's mapper matters: payloads then honour the same naming strategy,
 * date format and registered modules as the rest of the application's JSON.
 */
final class PayloadConverterFactory {

    private static final String JACKSON3_MAPPER = "tools.jackson.databind.ObjectMapper";

    private static final String JACKSON2_MAPPER = "com.fasterxml.jackson.databind.ObjectMapper";

    private PayloadConverterFactory() {
    }

    static PayloadConverter create(BeanFactory beanFactory) {
        ClassLoader classLoader = PayloadConverterFactory.class.getClassLoader();
        boolean jackson3 = ClassUtils.isPresent(JACKSON3_MAPPER, classLoader);
        boolean jackson2 = ClassUtils.isPresent(JACKSON2_MAPPER, classLoader);
        if (jackson3) {
            PayloadConverter fromBean = jackson3FromBean(beanFactory);
            if (fromBean != null) {
                return fromBean;
            }
        }
        if (jackson2) {
            PayloadConverter fromBean = jackson2FromBean(beanFactory);
            if (fromBean != null) {
                return fromBean;
            }
        }
        if (jackson3) {
            return new JacksonPayloadConverter();
        }
        if (jackson2) {
            return new Jackson2PayloadConverter();
        }
        throw new IllegalStateException(
                "PGMQ payloads must be JSON but no Jackson implementation was found. Add "
                        + "'tools.jackson.core:jackson-databind' (Jackson 3, the Spring Boot 4 default), or "
                        + "define your own PayloadConverter bean.");
    }

    private static @Nullable PayloadConverter jackson3FromBean(BeanFactory beanFactory) {
        try {
            return new JacksonPayloadConverter(beanFactory.getBean(tools.jackson.databind.ObjectMapper.class));
        }
        catch (BeansException ex) {
            return null;
        }
    }

    private static @Nullable PayloadConverter jackson2FromBean(BeanFactory beanFactory) {
        try {
            return new Jackson2PayloadConverter(
                    beanFactory.getBean(com.fasterxml.jackson.databind.ObjectMapper.class));
        }
        catch (BeansException ex) {
            return null;
        }
    }
}
