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
package gobblin.writer.test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.source.workunit.WorkUnit;
import gobblin.writer.Destination;
import gobblin.writer.Destination.DestinationType;


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
