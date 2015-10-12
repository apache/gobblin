/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.kafka;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


/**
 * Test {@link gobblin.metrics.kafka.KafkaPusher}.
 */
public class KafkaPusherTest extends KafkaTestBase {

  public static final String TOPIC = KafkaPusherTest.class.getSimpleName();

  public KafkaPusherTest()
      throws InterruptedException, RuntimeException {
    super(TOPIC);
  }

  @Test
  public void test() throws IOException {
    KafkaPusher pusher = new KafkaPusher("localhost:" + kafkaPort, TOPIC);

    String msg1 = "msg1";
    String msg2 = "msg2";

    pusher.pushMessages(Lists.newArrayList(msg1.getBytes(), msg2.getBytes()));

    try {
      Thread.sleep(1000);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), msg1);
    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), msg2);

    pusher.close();

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
