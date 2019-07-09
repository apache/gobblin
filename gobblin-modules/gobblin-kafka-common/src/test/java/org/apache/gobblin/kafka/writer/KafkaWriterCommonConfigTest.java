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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationException;
import org.apache.gobblin.types.AvroGenericRecordTypeMapper;
import org.apache.gobblin.util.ConfigUtils;


public class KafkaWriterCommonConfigTest {

  @Test
  public void testEmptyConstructor()
      throws ConfigurationException {
    Properties properties = new Properties();
    Config config = ConfigUtils.propertiesToConfig(properties);
    KafkaWriterCommonConfig kafkaWriterCommonConfig = new KafkaWriterCommonConfig(config);
    Assert.assertEquals(kafkaWriterCommonConfig.isKeyed(), false);
    Assert.assertEquals(kafkaWriterCommonConfig.getKeyField(), null);
    Assert.assertEquals(kafkaWriterCommonConfig.getTypeMapper().getClass().getCanonicalName(),
        AvroGenericRecordTypeMapper.class.getCanonicalName());
    Assert.assertEquals(kafkaWriterCommonConfig.getValueField(), "*");
  }

  @Test
  public void testKeyedConstructor() {
    Properties properties = new Properties();
    properties.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYED_CONFIG, "true");
    try {
      Config config = ConfigUtils.propertiesToConfig(properties);
      KafkaWriterCommonConfig kafkaWriterCommonConfig = new KafkaWriterCommonConfig(config);
      Assert.fail("Should fail to construct with keyed writes set to true, without setting key field");
    } catch (ConfigurationException ce) {
      // Expected
    }
    properties.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYFIELD_CONFIG, "key");
    try {
      Config config = ConfigUtils.propertiesToConfig(properties);
      KafkaWriterCommonConfig kafkaWriterCommonConfig = new KafkaWriterCommonConfig(config);
      Assert.assertEquals(kafkaWriterCommonConfig.isKeyed(), true);
      Assert.assertEquals(kafkaWriterCommonConfig.getKeyField(), "key");
      // Check default type mapper is AvroGenericRecord based
      Assert.assertEquals(kafkaWriterCommonConfig.getTypeMapper().getClass().getCanonicalName(),
          AvroGenericRecordTypeMapper.class.getCanonicalName());
      Assert.assertEquals(kafkaWriterCommonConfig.getValueField(), "*");
    } catch (ConfigurationException ce) {
      Assert.fail("Should successfully construct with keyed writes set to true, and with setting key field", ce);
    }
    properties.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_TYPEMAPPERCLASS_CONFIG, TestTypeMapper.class.getCanonicalName());
    try {
      Config config = ConfigUtils.propertiesToConfig(properties);
      KafkaWriterCommonConfig kafkaWriterCommonConfig = new KafkaWriterCommonConfig(config);
      Assert.assertEquals(kafkaWriterCommonConfig.isKeyed(), true);
      Assert.assertEquals(kafkaWriterCommonConfig.getKeyField(), "key");
      Assert.assertEquals(kafkaWriterCommonConfig.getTypeMapper().getClass().getCanonicalName(),
          TestTypeMapper.class.getCanonicalName());
      Assert.assertEquals(kafkaWriterCommonConfig.getValueField(), "*");
    } catch (ConfigurationException ce) {
      Assert.fail("Should successfully construct", ce);
    }
    properties.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_VALUEFIELD_CONFIG, "foobar");
    try {
      Config config = ConfigUtils.propertiesToConfig(properties);
      KafkaWriterCommonConfig kafkaWriterCommonConfig = new KafkaWriterCommonConfig(config);
      Assert.assertEquals(kafkaWriterCommonConfig.isKeyed(), true);
      Assert.assertEquals(kafkaWriterCommonConfig.getKeyField(), "key");
      Assert.assertEquals(kafkaWriterCommonConfig.getTypeMapper().getClass().getCanonicalName(),
          TestTypeMapper.class.getCanonicalName());
      Assert.assertEquals(kafkaWriterCommonConfig.getValueField(), "foobar");
    }
    catch (ConfigurationException ce) {
      Assert.fail("Should successfully construct", ce);
    }
  }


}
