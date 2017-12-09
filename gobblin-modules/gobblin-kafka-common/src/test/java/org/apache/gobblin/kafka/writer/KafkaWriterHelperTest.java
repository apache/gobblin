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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaWriterHelperTest {

  @Test
  public void testSharedConfig() {
    Properties props = new Properties();

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "key1", "value1");
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "key2", "value2");

    props.setProperty(ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX + ".key1", "sharedValue1");
    props.setProperty(ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX + ".key3", "sharedValue3");

    Properties producerProps = KafkaWriterHelper.getProducerProperties(props);

    // specific config overrides shared config
    Assert.assertEquals(producerProps.getProperty("key1"), "value1");
    Assert.assertEquals(producerProps.getProperty("key2"), "value2");
    Assert.assertEquals(producerProps.getProperty("key3"), "sharedValue3");
  }
}
