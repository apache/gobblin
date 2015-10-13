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

package gobblin.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.test.TestPartitionAwareWriterBuilder;
import gobblin.writer.test.TestPartitioner;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test {@link gobblin.writer.PartitionedDataWriter}
 */
public class PartitionedWriterTest {

  @Test
  public void test() throws IOException {

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TestPartitioner.class.getCanonicalName());

    TestPartitionAwareWriterBuilder builder = new TestPartitionAwareWriterBuilder();

    DataWriter<String> writer = new PartitionedDataWriter<String, String>(builder, state);

    Assert.assertEquals(builder.actions.size(), 0);

    String record1 = "abc";
    writer.write(record1);

    Assert.assertEquals(builder.actions.size(), 2);
    TestPartitionAwareWriterBuilder.Action action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.BUILD);

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record1);

    String record2 = "bcd";
    writer.write(record2);

    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "b");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.BUILD);

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "b");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record2);

    writer.write(record1);

    Assert.assertEquals(builder.actions.size(), 1);

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record1);

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), 3);

    writer.cleanup();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLEANUP);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLEANUP);

    writer.close();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLOSE);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLOSE);

    writer.commit();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.COMMIT);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.COMMIT);

  }

}
