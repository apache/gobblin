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

package org.apache.gobblin.source.extractor.extract.kafka;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


public class KafkaExtractorTest {

  @Test
  public void testGetKafkaBrokerSimpleName() {
    State state = new State();
    Assert.assertThrows(IllegalArgumentException.class, () -> KafkaExtractor.getKafkaBrokerSimpleName(state));
    state.setProp(ConfigurationKeys.KAFKA_BROKERS, "");
    Assert.assertThrows(IllegalArgumentException.class, () -> KafkaExtractor.getKafkaBrokerSimpleName(state));

    final String kafkaBrokerUri = "kafka.broker.uri.com:12345";
    final String kafkaBrokerSimpleName = "simple.kafka.name";
    state.setProp(ConfigurationKeys.KAFKA_BROKERS, kafkaBrokerUri);
    Assert.assertEquals("", KafkaExtractor.getKafkaBrokerSimpleName(state));

    state.setProp(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY, String.format("foobar->foobarId", kafkaBrokerUri, kafkaBrokerSimpleName));
    Assert.assertEquals("", KafkaExtractor.getKafkaBrokerSimpleName(state));

    state.setProp(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY, String.format("%s->%s,foobar->foobarId", kafkaBrokerUri, kafkaBrokerSimpleName));
    Assert.assertEquals(KafkaExtractor.getKafkaBrokerSimpleName(state), kafkaBrokerSimpleName);
  }

  @Test
  public void testSimpleMapKeyIsBackwardCompatible() {
    Config cfg = ConfigFactory.empty()
        .withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("kafkaBrokerUri"))
        .withValue(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY,
            ConfigValueFactory.fromAnyRef("kafkaBrokerUri->simpleName"));

    Assert.assertEquals(cfg.getString(ConfigurationKeys.KAFKA_BROKERS), "kafkaBrokerUri");
    Assert.assertEquals(cfg.getString(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY), "kafkaBrokerUri->simpleName");
  }
}
