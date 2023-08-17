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

package org.apache.gobblin;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


public class KafkaCommonUtilTest {

  @Test
  public void testGetKafkaBrokerToSimpleNameMap() {
    String brokerUri =  "kafka.some-identifier.kafka.coloc-123.com:12345";
    String simpleName = "some-identifier";

    State state = new State();
    Assert.assertEquals(KafkaCommonUtil.getKafkaBrokerToSimpleNameMap(state).size(),0);

    state.setProp(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY, String.format("%s->%s", brokerUri, simpleName));
    Assert.assertEquals(KafkaCommonUtil.getKafkaBrokerToSimpleNameMap(state),
        ImmutableMap.of(brokerUri, simpleName));

    state.setProp(ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY,
        String.format("foobar.com:12345->foobar,%s->%s", brokerUri, simpleName));
    Assert.assertEquals(KafkaCommonUtil.getKafkaBrokerToSimpleNameMap(state),
        ImmutableMap.of(brokerUri, simpleName, "foobar.com:12345", "foobar"));
  }
}
