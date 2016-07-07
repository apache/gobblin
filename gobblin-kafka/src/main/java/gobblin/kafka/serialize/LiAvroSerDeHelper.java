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

package gobblin.kafka.serialize;

import java.util.Map;
import java.util.Properties;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;


/**
 * Helper class for {@link LiAvroSerializer} and {@link LiAvroDeserializer}.
 */
public class LiAvroSerDeHelper {
  public static final byte MAGIC_BYTE = 0x0;

  public static KafkaSchemaRegistry getSchemaRegistry(Map<String, ?> config) {
    Properties props = new Properties();
    props.putAll(config);
    return KafkaSchemaRegistryFactory.getSchemaRegistry(props);
  }
}
