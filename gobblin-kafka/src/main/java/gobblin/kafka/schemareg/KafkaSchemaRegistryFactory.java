/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.schemareg;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;


/**
 * A Factory that constructs and hands back {@link KafkaSchemaRegistry} implementations.
 */
@Slf4j
public class KafkaSchemaRegistryFactory {

  @SuppressWarnings("unchecked")
  public static KafkaSchemaRegistry getSchemaRegistry(Properties props) {
    Preconditions.checkArgument(props.containsKey(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS),
        "Missing required property " + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS);

    boolean tryCache = true;
    if (props.containsKey(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CACHE))
    {
      tryCache = Boolean.parseBoolean(props.getProperty(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CACHE, "false"));
    }

    Class<?> clazz;
    try {
      clazz =
          (Class<?>) Class.forName(props.getProperty(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS));
      KafkaSchemaRegistry schemaRegistry = (KafkaSchemaRegistry) ConstructorUtils.invokeConstructor(clazz, props);
      if (tryCache && !schemaRegistry.hasInternalCache())
      {
        // TODO: Wrap the schema registry with an caching schema registry
      }
      return schemaRegistry;
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException
        | InstantiationException e) {
      log.error("Failed to instantiate " + KafkaSchemaRegistry.class, e);
      throw Throwables.propagate(e);
    }
  }
}
