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
package org.apache.gobblin.kafka.writer;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationException;
import org.apache.gobblin.types.TypeMapper;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;


/**
 * Version-independent configuration for Kafka Writers
 */
public class KafkaWriterCommonConfig {

  @Getter
  private final boolean keyed;
  @Getter
  private final String keyField;
  @Getter
  private final TypeMapper typeMapper;
  @Getter
  private final String valueField;

  public KafkaWriterCommonConfig(Config config)
      throws ConfigurationException {
    try {
      this.keyed = ConfigUtils.getBoolean(config, WRITER_KAFKA_KEYED_CONFIG, WRITER_KAFKA_KEYED_DEFAULT);
      this.keyField = ConfigUtils.getString(config, WRITER_KAFKA_KEYFIELD_CONFIG, WRITER_KAFKA_KEYFIELD_DEFAULT);
      String typeMapperClass = ConfigUtils.getString(config, WRITER_KAFKA_TYPEMAPPERCLASS_CONFIG,
          WRITER_KAFKA_TYPEMAPPERCLASS_DEFAULT);
      this.typeMapper = (TypeMapper) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(typeMapperClass));
      this.valueField = ConfigUtils.getString(config, WRITER_KAFKA_VALUEFIELD_CONFIG, WRITER_KAFKA_VALUEFIELD_DEFAULT);
      Preconditions.checkArgument(!this.keyed || (this.keyField != null),
          "With keyed writes to Kafka, you must provide a key fieldname to be used");
    } catch (Exception e) {
      throw new ConfigurationException("Failed to configure KafkaWriterCommonConfig", e);
    }
  }
}
