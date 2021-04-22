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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.ConsumerIterator;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.metrics.kafka.KafkaProducerPusher;
import org.apache.gobblin.metrics.kafka.Pusher;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.gobblin.KafkaCommonUtil.*;


/**
 * Test {@link org.apache.gobblin.metrics.kafka.KafkaProducerPusher}.
 */
public class KafkaProducerPusherTest {
  public static final String TOPIC = KafkaProducerPusherTest.class.getSimpleName();

  private org.apache.gobblin.kafka.KafkaTestBase kafkaTestHelper;

  private final long flushTimeoutMilli = KAFKA_FLUSH_TIMEOUT_SECONDS * 1000;

  @BeforeClass
  public void setup() throws Exception {
    kafkaTestHelper = new KafkaTestBase();
    kafkaTestHelper.startServers();

    kafkaTestHelper.provisionTopic(TOPIC);
  }

  @Test(priority = 0)
  public void testPushMessages() throws IOException {
    // Test that the scoped config overrides the generic config
    Pusher pusher = new KafkaProducerPusher("127.0.0.1:dummy", TOPIC, Optional.of(ConfigFactory.parseMap(ImmutableMap.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + this.kafkaTestHelper.getKafkaServerPort()))));

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

  @Test(priority = 1)
  public void testCloseTimeOut() throws IOException {
    // Test that the scoped config overrides the generic config
    Pusher pusher = new KafkaProducerPusher("127.0.0.1:dummy", TOPIC, Optional.of(ConfigFactory.parseMap(ImmutableMap.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + this.kafkaTestHelper.getKafkaServerPort()))));

    Runnable stuffToDo = new Thread() {
      @Override
      public void run() {
        final long startRunTime = System.currentTimeMillis();
        String msg = "msg";
        ArrayList al = Lists.newArrayList(msg.getBytes());
        // Keep push messages that last 2 times longer than close timeout
        while ( System.currentTimeMillis() - startRunTime < flushTimeoutMilli * 2) {
          pusher.pushMessages(al);
        }
      }
    };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(stuffToDo);
    try {
      Thread.sleep(1000);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    long startCloseTime = System.currentTimeMillis();
    pusher.close();
    // Assert that the close should be performed around the timeout, even more messages being pushed
    Assert.assertTrue(System.currentTimeMillis() - startCloseTime < flushTimeoutMilli + 3000);
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
