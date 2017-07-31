/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.kafka.serialize;

import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;


/**
 * Helper class for {@link LiAvroSerializer} and {@link LiAvroDeserializerBase}.
 */
public class LiAvroSerDeHelper {
  public static final byte MAGIC_BYTE = 0x0;

  public static KafkaSchemaRegistry getSchemaRegistry(Map<String, ?> config) {
    Properties props = new Properties();
    props.putAll(config);
    return KafkaSchemaRegistryFactory.getSchemaRegistry(props);
  }
}
