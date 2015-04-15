/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@Test(groups = {"gobblin.metrics"})
public class KafkaTestBaseTest extends KafkaTestBase {

  public KafkaTestBaseTest()
      throws InterruptedException, RuntimeException {
    super("test");
  }

  @Test
  public void test() {

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:" + kafkaPort);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<String, String>(config);

    producer.send(new KeyedMessage<String, String>("test", "testMessage"));

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), "testMessage");

    producer.close();
  }

  @AfterClass
  public void after() {
    try {
      close();
    } catch(Exception e) {
      System.err.println("Failed to close Kafka server.");
    }
  }

  @AfterSuite
  public void afterSuite() {
    closeServer();
  }
}
