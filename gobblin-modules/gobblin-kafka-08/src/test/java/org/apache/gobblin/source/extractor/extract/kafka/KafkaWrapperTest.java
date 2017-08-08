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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

@Slf4j
public class KafkaWrapperTest {

  @Test
  public void testTimeoutConfig()
  {
    String brokerList = "localhost:9092";
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.KAFKA_BROKERS, brokerList);
    props.setProperty("source.kafka.fetchTimeoutMillis", "10000");
    props.setProperty("source.kafka.socketTimeoutMillis", "1000");
    State state = new State(props);
    try {
      KafkaWrapper wrapper = KafkaWrapper.create(state);
      Assert.fail("KafkaWrapper should fail to initialize if fetchTimeout is greater than socketTimeout");
    }
    catch (IllegalArgumentException e)
    {
      log.info("Found exception as expected");
      log.debug("Exception trace", e);
    }
    catch (Exception e)
    {
      Assert.fail("Should only throw IllegalArgumentException", e);
    }
  }
}
