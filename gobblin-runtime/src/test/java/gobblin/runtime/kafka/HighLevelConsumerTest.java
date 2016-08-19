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

package gobblin.runtime.kafka;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.runtime.job_monitor.MockKafkaStream;


public class HighLevelConsumerTest {

  public static Config getSimpleConfig(Optional<String> prefix) {
    Properties properties = new Properties();
    properties.put(getConfigKey(prefix, "zookeeper.connect"), "zookeeper");

    return ConfigFactory.parseProperties(properties);
  }

  private static String getConfigKey(Optional<String> prefix, String key) {
    return prefix.isPresent() ? prefix.get() + "." + key : key;
  }

  @Test
  public void test() throws Exception {

    MockKafkaStream mockKafkaStream = new MockKafkaStream(5);
    MockedHighLevelConsumer consumer = new MockedHighLevelConsumer(getSimpleConfig(Optional.<String>absent()), 5, mockKafkaStream);

    consumer.startAsync();
    consumer.awaitRunning();

    Assert.assertTrue(consumer.getMessages().isEmpty());

    mockKafkaStream.pushToStream("message");

    consumer.awaitAtLeastNMessages(1);
    Assert.assertEquals(consumer.getMessages().get(0), "message");

    mockKafkaStream.pushToStream("message2");
    consumer.awaitAtLeastNMessages(2);
    Assert.assertEquals(consumer.getMessages().get(1), "message2");

    consumer.shutDown();
    mockKafkaStream.pushToStream("message3");
    try {
      consumer.awaitAtLeastNMessages(3);
      Assert.fail();
    } catch (TimeoutException ie) {
      // should throw this
    }
  }
}
