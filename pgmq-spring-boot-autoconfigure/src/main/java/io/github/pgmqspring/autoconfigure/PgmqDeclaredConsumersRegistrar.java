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

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.PgmqAcknowledgingMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqBatchMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqMessageHandler;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

/**
 * Registers a {@link PgmqMessageListenerContainer} bean, named {@code pgmqConsumer-<name>}, for
 * every entry under {@code pgmq.consumers}.
 *
 * <p>Each entry is bound in two layers - {@code pgmq.consumer.*}, then its own keys on top - so it
 * inherits every default and overrides exactly what it sets. Bean definitions are registered
 * before any bean exists, which is why the entries are bound from the {@code Environment} rather
 * than read from {@link PgmqProperties}; the containers themselves, and the checks that need other
 * beans, are created later with every other singleton.
 */
class PgmqDeclaredConsumersRegistrar implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    static final String BEAN_NAME_PREFIX = "pgmqConsumer-";

    private static final String PREFIX = "pgmq.consumers";

    private @Nullable Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        if (!(registry instanceof ConfigurableListableBeanFactory beanFactory)) {
            return;
        }
        Binder binder = Binder.get(requireEnvironment());
        // The exact name of each entry, to bind it again on top of the defaults: a name rebuilt from
        // the map key would not match keys Boot does not hold in canonical form, such as orders_v2.
        Map<PgmqProperties.DeclaredConsumer, ConfigurationPropertyName> entryNames = new IdentityHashMap<>();
        BindHandler recordEntryNames = new BindHandler() {
            @Override
            public Object onSuccess(ConfigurationPropertyName name, Bindable<?> target, BindContext context,
                    Object result) {
                if (result instanceof PgmqProperties.DeclaredConsumer entry) {
                    entryNames.put(entry, name);
                }
                return result;
            }
        };
        Map<String, PgmqProperties.DeclaredConsumer> declared = binder
                .bind(PREFIX, Bindable.mapOf(String.class, PgmqProperties.DeclaredConsumer.class), recordEntryNames)
                .orElse(Map.of());
        Map<String, String> consumersByQueue = new LinkedHashMap<>();
        declared.forEach((name, consumer) -> {
            String queue = QueueNames.validate(consumer.getQueue() != null ? consumer.getQueue() : name);
            String previous = consumersByQueue.put(QueueNames.normalize(queue), name);
            if (previous != null) {
                throw new IllegalStateException(PREFIX + "." + previous + " and " + PREFIX + "." + name
                        + " both consume queue '" + queue + "'. Declare it once, and raise its concurrency instead.");
            }
            if (!StringUtils.hasText(consumer.getHandler())) {
                throw new IllegalStateException(PREFIX + "." + name + ".handler is required: the name of the "
                        + "handler bean that consumes queue '" + queue + "'");
            }
            String beanName = BEAN_NAME_PREFIX + name;
            if (registry.containsBeanDefinition(beanName)) {
                throw new IllegalStateException("Cannot create the container for " + PREFIX + "." + name
                        + ": a bean named '" + beanName + "' already exists");
            }
            RootBeanDefinition definition = new RootBeanDefinition(PgmqMessageListenerContainer.class);
            ConfigurationPropertyName entryName = entryNames.get(consumer);
            definition.setInstanceSupplier(() -> createContainer(beanFactory, binder, name, entryName, beanName));
            registry.registerBeanDefinition(beanName, definition);
        });
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
    }

    private Environment requireEnvironment() {
        Environment current = this.environment;
        if (current == null) {
            throw new IllegalStateException("no Environment was set");
        }
        return current;
    }

    private static PgmqMessageListenerContainer<?> createContainer(ConfigurableListableBeanFactory beanFactory,
            Binder binder, String name, @Nullable ConfigurationPropertyName entryName, String beanName) {
        String property = PREFIX + "." + name;
        PgmqProperties.DeclaredConsumer consumer = new PgmqProperties.DeclaredConsumer();
        binder.bind("pgmq.consumer", Bindable.ofInstance(consumer));
        if (entryName != null) {
            binder.bind(entryName, Bindable.ofInstance(consumer));
        }
        String queue = consumer.getQueue() != null ? consumer.getQueue() : name;
        String handlerName = consumer.getHandler();
        if (handlerName == null || !beanFactory.containsBean(handlerName)) {
            throw new IllegalStateException(property + ".handler names bean '" + handlerName
                    + "', which does not exist");
        }
        Object handler = beanFactory.getBean(handlerName);
        Class<?> payloadType = payloadType(beanFactory, property, handlerName, handler, consumer.getPayloadType());
        ConsumerOptions options;
        try {
            options = PgmqAutoConfiguration.consumerOptions(consumer);
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalStateException(property + ": " + ex.getMessage(), ex);
        }
        PgmqMessageListenerContainer<?> container = build(beanFactory.getBean(PgmqOperations.class), queue,
                payloadType, options, transactionManager(beanFactory, property, consumer), handler, property);
        container.setBeanName(beanName);
        container.setAutoStartup(consumer.isAutoStartup());
        return container;
    }

    private static @Nullable PlatformTransactionManager transactionManager(ConfigurableListableBeanFactory beanFactory,
            String property, PgmqProperties.DeclaredConsumer consumer) {
        String name = consumer.getTransactionManager();
        if (name == null) {
            return beanFactory.getBeanProvider(PlatformTransactionManager.class).getIfUnique();
        }
        if (!beanFactory.containsBean(name)) {
            throw new IllegalStateException(property + ".transaction-manager names bean '" + name
                    + "', which does not exist");
        }
        return beanFactory.getBean(name, PlatformTransactionManager.class);
    }

    /**
     * The payload type: the configured one, checked against the handler's type argument, or that
     * type argument when none is configured.
     */
    private static Class<?> payloadType(ConfigurableListableBeanFactory beanFactory, String property,
            String handlerName, Object handler, @Nullable Class<?> configured) {
        Class<?> declared = handledType(beanFactory, handlerName, handler);
        if (configured == null) {
            if (declared == null) {
                throw new IllegalStateException(property + ".payload-type is required: the payload type of handler "
                        + "bean '" + handlerName + "' cannot be determined from its type, as for a lambda");
            }
            return declared;
        }
        if (declared != null && !declared.isAssignableFrom(configured)) {
            throw new IllegalStateException(property + ".payload-type is " + configured.getName() + ", but handler "
                    + "bean '" + handlerName + "' handles " + declared.getName());
        }
        return configured;
    }

    /**
     * The type argument of the handler interface the bean implements, from its bean definition -
     * which keeps the generics of a {@code @Bean} method's return type - or else from its class.
     */
    private static @Nullable Class<?> handledType(ConfigurableListableBeanFactory beanFactory, String handlerName,
            Object handler) {
        ResolvableType definitionType = beanFactory.containsBeanDefinition(handlerName)
                ? beanFactory.getMergedBeanDefinition(handlerName).getResolvableType()
                : ResolvableType.NONE;
        for (ResolvableType type : new ResolvableType[] {definitionType,
                ResolvableType.forClass(ClassUtils.getUserClass(handler))}) {
            for (Class<?> handlerInterface : new Class<?>[] {PgmqMessageHandler.class,
                    PgmqAcknowledgingMessageHandler.class, PgmqBatchMessageHandler.class}) {
                Class<?> resolved = type.as(handlerInterface).getGeneric(0).resolve();
                if (resolved != null) {
                    return resolved;
                }
            }
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static PgmqMessageListenerContainer<?> build(PgmqOperations pgmq, String queue, Class<?> payloadType,
            ConsumerOptions options, @Nullable PlatformTransactionManager transactionManager, Object handler,
            String property) {
        PgmqMessageListenerContainer.Builder builder = PgmqMessageListenerContainer.builder(pgmq, queue, payloadType)
                .options(options)
                .transactionManager(transactionManager);
        if (handler instanceof PgmqBatchMessageHandler batchHandler) {
            builder.batchHandler(batchHandler);
        }
        else if (handler instanceof PgmqAcknowledgingMessageHandler acknowledgingHandler) {
            builder.acknowledgingHandler(acknowledgingHandler);
        }
        else if (handler instanceof PgmqMessageHandler messageHandler) {
            builder.handler(messageHandler);
        }
        else {
            throw new IllegalStateException(property + ".handler names a " + handler.getClass().getName()
                    + ", which is not a PgmqMessageHandler, PgmqAcknowledgingMessageHandler or PgmqBatchMessageHandler");
        }
        try {
            return builder.build();
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalStateException(property + ": " + ex.getMessage(), ex);
        }
    }
}
