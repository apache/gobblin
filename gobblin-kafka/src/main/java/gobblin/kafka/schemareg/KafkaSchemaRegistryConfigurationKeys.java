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

/**
 * Configuration keys for a kafka schema registry
 */
public class KafkaSchemaRegistryConfigurationKeys {
  public final static String KAFKA_SCHEMA_REGISTRY_CLASS = "kafka.schemaRegistry.class";
  public final static String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schemaRegistry.url";
  public final static String KAFKA_SCHEMA_REGISTRY_CACHE = "kafka.schemaRegistry.cache";
}
