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
package org.apache.gobblin.metrics.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.gobblin.metrics.reporter.KeyValuePusher;


@Test
public class LoggingPusherTest {

  @Test
  public void testKafkaReporter() {

    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getLogger(LoggingPusher.class.getName());
    logger.setLevel(Level.INFO);
    logger.addAppender(testAppender);

    KeyValuePusher<String, String> loggingPusher =
        new LoggingPusher<String, String>("broker", "topic", Optional.absent());

    loggingPusher.pushMessages(ImmutableList.of("message1", "message2"));
    loggingPusher.pushKeyValueMessages(ImmutableList.of(org.apache.commons.lang3.tuple.Pair.of("key", "message3")));

    Assert.assertEquals(testAppender.events.size(), 3);
    Assert.assertEquals(testAppender.events.get(0).getRenderedMessage(), "Pushing to broker:topic: message1");
    Assert.assertEquals(testAppender.events.get(1).getRenderedMessage(), "Pushing to broker:topic: message2");
    Assert.assertEquals(testAppender.events.get(2).getRenderedMessage(), "Pushing to broker:topic: key - message3");

    logger.removeAppender(testAppender);
  }

  private class TestAppender extends AppenderSkeleton {
    List<LoggingEvent> events = new ArrayList<LoggingEvent>();

    public void close() {
    }

    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }
  }
}
