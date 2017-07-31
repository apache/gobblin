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
package org.apache.gobblin.writer.test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.Destination.DestinationType;


/**
 * Unit tests for {@link GobblinTestEventBusWriter}
 */
public class GobblinTestEventBusWriterTest {

  @Test
  public void testWrite() throws IOException, InterruptedException, TimeoutException {
    final String eventBusId = "/tmp/GobblinTestEventBusWriterTest/testWrite";

    try(TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId)) {
      GobblinTestEventBusWriter writer =
          GobblinTestEventBusWriter.builder().withEventBusId(eventBusId).build();

      writer.write("event1");
      writer.write("event2");
      writer.write("event3");

      asserter.assertNextValueEq("event1");
      asserter.assertNextValueEq("event2");
      asserter.assertNextValueEq("event3");

      Assert.assertEquals(writer.recordsWritten(), 3);
    }
  }

  @Test
  public void testBuilder() throws IOException, InterruptedException, TimeoutException {
    final String eventBusId = "/GobblinTestEventBusWriterTest/testBuilder";

    GobblinTestEventBusWriter.Builder writerBuilder = new GobblinTestEventBusWriter.Builder();
    WorkUnit wu = WorkUnit.createEmpty();
    wu.setProp(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId);
    writerBuilder.writeTo(Destination.of(DestinationType.HDFS, wu));

    Assert.assertEquals(writerBuilder.getEventBusId(), eventBusId);

    try(TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId)) {
      GobblinTestEventBusWriter writer = writerBuilder.build();

      writer.write("event1");
      writer.write("event2");

      asserter.assertNextValueEq("event1");
      asserter.assertNextValueEq("event2");

      Assert.assertEquals(writer.recordsWritten(), 2);
    }
  }

}
