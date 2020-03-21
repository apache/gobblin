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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import org.apache.gobblin.ack.BasicAckableForTesting;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.PartitionDescriptor;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.writer.test.TestPartitionAwareWriterBuilder;
import org.apache.gobblin.writer.test.TestPartitioner;


/**
 * Test {@link org.apache.gobblin.writer.PartitionedDataWriter}
 */
public class PartitionedWriterTest {

  @Test
  public void test() throws IOException {

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TestPartitioner.class.getCanonicalName());

    TestPartitionAwareWriterBuilder builder = new TestPartitionAwareWriterBuilder();

    PartitionedDataWriter writer = new PartitionedDataWriter<String, String>(builder, state);

    Assert.assertEquals(builder.actions.size(), 0);

    String record1 = "abc";
    writer.writeEnvelope(new RecordEnvelope(record1));

    Assert.assertEquals(builder.actions.size(), 2);
    TestPartitionAwareWriterBuilder.Action action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.BUILD);

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record1);
    Assert.assertTrue(writer.isSpeculativeAttemptSafe());

    String record2 = "123";
    writer.writeEnvelope(new RecordEnvelope(record2));

    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "1");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.BUILD);
    Assert.assertFalse(writer.isSpeculativeAttemptSafe());

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "1");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record2);

    writer.writeEnvelope(new RecordEnvelope(record1));

    Assert.assertEquals(builder.actions.size(), 1);

    action = builder.actions.poll();
    Assert.assertEquals(action.getPartition(), "a");
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.WRITE);
    Assert.assertEquals(action.getTarget(), record1);

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), 3);
    Assert.assertFalse(writer.isSpeculativeAttemptSafe());

    writer.cleanup();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLEANUP);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLEANUP);

    // Before close, partitions info is not serialized
    String partitionsKey = "writer.0.partitions";
    Assert.assertTrue(state.getProp(partitionsKey) == null);

    writer.close();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLOSE);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.CLOSE);

    // After close, partitions info is available
    Assert.assertFalse(Strings.isNullOrEmpty(state.getProp(partitionsKey)));
    List<PartitionDescriptor> partitions = PartitionedDataWriter.getPartitionInfoAndClean(state, 0);
    Assert.assertTrue(state.getProp(partitionsKey) == null);
    Assert.assertEquals(partitions.size(), 2);

    DatasetDescriptor dataset = new DatasetDescriptor("testPlatform", "testDataset");
    Assert.assertEquals(partitions.get(0), new PartitionDescriptor("a", dataset));
    Assert.assertEquals(partitions.get(1), new PartitionDescriptor("1", dataset));

    writer.commit();
    Assert.assertEquals(builder.actions.size(), 2);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.COMMIT);
    action = builder.actions.poll();
    Assert.assertEquals(action.getType(), TestPartitionAwareWriterBuilder.Actions.COMMIT);
  }


  @Test
  public void testControlMessageHandler() throws IOException {

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TestPartitioner.class.getCanonicalName());

    TestPartitionAwareWriterBuilder builder = new TestPartitionAwareWriterBuilder();

    PartitionedDataWriter writer = new PartitionedDataWriter<String, String>(builder, state);

    Assert.assertEquals(builder.actions.size(), 0);

    String record1 = "abc";
    writer.writeEnvelope(new RecordEnvelope(record1));

    String record2 = "123";
    writer.writeEnvelope(new RecordEnvelope(record2));

    FlushControlMessage controlMessage = FlushControlMessage.builder().build();
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    controlMessage.addCallBack(ackable);
    Assert.assertEquals(ackable.acked, 0);

    // when the control message is cloned properly then this does not raise an error
    writer.getMessageHandler().handleMessage(controlMessage);

    // message handler does not ack since consumeRecordStream does acking for control messages
    // this should be revisited when control message error handling is changed
    controlMessage.ack();

    Assert.assertEquals(ackable.acked, 1);

    writer.close();
  }

  @Test
  public void testPartitionWriterCacheRemovalListener()
      throws IOException, InterruptedException {
    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TestPartitioner.class.getCanonicalName());
    state.setProp(PartitionedDataWriter.PARTITIONED_WRITER_CACHE_TTL_SECONDS, 1);
    TestPartitionAwareWriterBuilder builder = new TestPartitionAwareWriterBuilder();

    PartitionedDataWriter writer = new PartitionedDataWriter<String, String>(builder, state);

    String record1 = "abc";
    writer.writeEnvelope(new RecordEnvelope(record1));

    String record2 = "123";
    writer.writeEnvelope(new RecordEnvelope(record2));

    //Sleep for more than cache expiration interval
    Thread.sleep(1500);

    //Call cache clean up to ensure removal of expired entries.
    writer.getPartitionWriters().cleanUp();

    //Ensure the removal listener updates counters.
    Assert.assertEquals(writer.getTotalRecordsFromEvictedWriters(), 2L);
    Assert.assertEquals(writer.getTotalBytesFromEvictedWriters(), 2L);
  }
}
