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

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.gobblin.metrics.reporter.KeyValuePusher;


@Test
public class LoggingPusherTest {

  @Test
  public void testKafkaReporter() {

    TestAppender testAppender = new TestAppender("TestAppender" );
    Logger logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(LoggingPusher.class.getName());
    Configurator.setRootLevel(Level.INFO);
    logger.addAppender(testAppender);

    KeyValuePusher<String, String> loggingPusher =
        new LoggingPusher<String, String>("broker", "topic", Optional.absent());

    loggingPusher.pushMessages(ImmutableList.of("message1", "message2"));
    loggingPusher.pushKeyValueMessages(ImmutableList.of(org.apache.commons.lang3.tuple.Pair.of("key", "message3")));

    Assert.assertEquals(testAppender.events.size(), 3);
    Assert.assertEquals(testAppender.events.get(0).getMessage().getFormattedMessage(), "Pushing to broker:topic: message1");
    Assert.assertEquals(testAppender.events.get(1).getMessage().getFormattedMessage(), "Pushing to broker:topic: message2");
    Assert.assertEquals(testAppender.events.get(2).getMessage().getFormattedMessage(), "Pushing to broker:topic: key - message3");

    logger.removeAppender(testAppender);
  }

  public class TestAppender extends AbstractAppender {

    private List<LogEvent> events = new ArrayList<LogEvent>();

    public TestAppender(String name) {
      super(name, null, null);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event);
    }
  }


}
