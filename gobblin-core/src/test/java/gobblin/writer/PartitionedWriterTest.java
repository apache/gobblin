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

package gobblin.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.writer.test.TestPartitionAwareWriterBuilder;
import gobblin.writer.test.TestPartitioner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test {@link gobblin.writer.PartitionedDataWriter}
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

  @Test
  public void testWatermarkComputation() throws IOException {
    testWatermarkComputation(0L, 1L, 0L);
    testWatermarkComputation(1L, 0L, null);
    testWatermarkComputation(0L, 0L, null);
    testWatermarkComputation(20L, 1L, null);
  }

  public void testWatermarkComputation(Long committed, Long unacknowledged, Long expected) throws IOException {
    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TestPartitioner.class.getCanonicalName());

    String defaultSource = "default";

    WatermarkAwareWriter mockDataWriter = mock(WatermarkAwareWriter.class);
    when(mockDataWriter.isWatermarkCapable()).thenReturn(true);
    when(mockDataWriter.getCommittableWatermark()).thenReturn(Collections.singletonMap(defaultSource,
        new DefaultCheckpointableWatermark(defaultSource, new LongWatermark(committed))));
    when(mockDataWriter.getUnacknowledgedWatermark()).thenReturn(Collections.singletonMap(defaultSource,
        new DefaultCheckpointableWatermark(defaultSource, new LongWatermark(unacknowledged))));

    PartitionAwareDataWriterBuilder builder = mock(PartitionAwareDataWriterBuilder.class);
    when(builder.validatePartitionSchema(any(Schema.class))).thenReturn(true);
    when(builder.forPartition(any(GenericRecord.class))).thenReturn(builder);
    when(builder.withWriterId(any(String.class))).thenReturn(builder);
    when(builder.build()).thenReturn(mockDataWriter);

    PartitionedDataWriter writer = new PartitionedDataWriter<String, String>(builder, state);

    RecordEnvelope<String> recordEnvelope = new RecordEnvelope<String>("0").withAckableWatermark(
        new AcknowledgableWatermark(new DefaultCheckpointableWatermark(defaultSource, new LongWatermark(0))));
    writer.writeEnvelope(recordEnvelope);

    Map<String, CheckpointableWatermark> watermark = writer.getCommittableWatermark();
    System.out.println(watermark.toString());
    if (expected == null) {
      Assert.assertTrue(watermark.isEmpty(), "Expected watermark to be absent");
    } else {
      Assert.assertTrue(watermark.size() == 1);
      Assert.assertEquals((long) expected, ((LongWatermark) watermark.values().iterator().next().getWatermark()).getValue());
    }
  }

}
