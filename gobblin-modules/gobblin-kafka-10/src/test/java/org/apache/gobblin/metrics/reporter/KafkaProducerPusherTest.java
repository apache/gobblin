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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.metrics.kafka.KafkaProducerPusher;
import org.apache.gobblin.metrics.kafka.Pusher;

import kafka.consumer.ConsumerIterator;


/**
 * Test {@link org.apache.gobblin.metrics.kafka.KafkaProducerPusher}.
 */
public class KafkaProducerPusherTest {
  public static final String TOPIC = KafkaProducerPusherTest.class.getSimpleName();

  private KafkaTestBase kafkaTestHelper;

  @BeforeClass
  public void setup() throws Exception {
    kafkaTestHelper = new KafkaTestBase();
    kafkaTestHelper.startServers();

    kafkaTestHelper.provisionTopic(TOPIC);
  }

  @Test
  public void test() throws IOException {
    // Test that the scoped config overrides the generic config
    Pusher pusher = new KafkaProducerPusher("localhost:dummy", TOPIC, Optional.of(ConfigFactory.parseMap(ImmutableMap.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + this.kafkaTestHelper.getKafkaServerPort()))));

    String msg1 = "msg1";
    String msg2 = "msg2";

    pusher.pushMessages(Lists.newArrayList(msg1.getBytes(), msg2.getBytes()));

    try {
      Thread.sleep(1000);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    ConsumerIterator<byte[], byte[]> iterator = this.kafkaTestHelper.getIteratorForTopic(TOPIC);

    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), msg1);
    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), msg2);

    pusher.close();
  }

  @AfterClass
  public void after() {
    try {
      this.kafkaTestHelper.close();
    } catch(Exception e) {
      System.err.println("Failed to close Kafka server.");
    }
  }
}
