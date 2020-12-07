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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.extract.FlushingExtractor;


public class KafkaStreamingExtractorTest {
  private KafkaStreamingExtractor streamingExtractor;
  private final int numPartitions = 3;

  @BeforeClass
  public void setUp() {
    WorkUnitState state = KafkaExtractorUtils.getWorkUnitState("testTopic", numPartitions);
    state.setProp(FlushingExtractor.FLUSH_DATA_PUBLISHER_CLASS, TestDataPublisher.class.getName());
    this.streamingExtractor = new KafkaStreamingExtractor(state);
  }

  @Test
  public void testResetExtractorStats()
      throws IOException, DataRecordException {
    MultiLongWatermark highWatermark1 = new MultiLongWatermark(this.streamingExtractor.highWatermark);

    //Read 2 records
    this.streamingExtractor.readStreamEntityImpl();
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(0), 1L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(1), 0L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(2), 0L);

    this.streamingExtractor.readStreamEntityImpl();
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(0), 1L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(1), 1L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(2), 0L);

    //Checkpoint watermarks
    this.streamingExtractor.onFlushAck();

    //Reset extractor stats
    this.streamingExtractor.resetExtractorStatsAndWatermarks(false);

    //Ensure post-reset invariance is satisfied i.e. low watermark and next watermark are identical.
    testAfterReset(highWatermark1);

    MultiLongWatermark highWatermark2 = new MultiLongWatermark(this.streamingExtractor.highWatermark);
    //Read 1 more record
    this.streamingExtractor.readStreamEntityImpl();
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(0), 1L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(1), 1L);
    Assert.assertEquals( this.streamingExtractor.nextWatermark.get(2), 1L);

    Assert.assertEquals( this.streamingExtractor.lowWatermark.get(0), 1L);
    Assert.assertEquals( this.streamingExtractor.lowWatermark.get(1), 1L);
    Assert.assertEquals( this.streamingExtractor.lowWatermark.get(2), 0L);

    //Checkpoint watermarks
    this.streamingExtractor.onFlushAck();

    //Reset extractor stats
    this.streamingExtractor.resetExtractorStatsAndWatermarks(false);

    //Ensure post-reset invariance is satisfied.
    testAfterReset(highWatermark2);
  }

  private void testAfterReset(MultiLongWatermark previousHighWatermark) {
    //Ensure that low and next watermarks are identical after reset. Also ensure the new high watermark is greater than
    // the previous high watermark.
    for (int i=0; i < numPartitions; i++) {
      Assert.assertEquals(this.streamingExtractor.lowWatermark.get(i), this.streamingExtractor.nextWatermark.get(i));
      Assert.assertTrue(previousHighWatermark.get(i) <= this.streamingExtractor.highWatermark.get(i));
    }
  }

  @Test
  public void testGenerateAdditionalTagHelper() throws Exception {
    // Verifying that produce rate has been added.
    Map<KafkaPartition, Map<String, String>> result = this.streamingExtractor.getAdditionalTagsHelper();
    for (Map<String, String> entry: result.values()) {
      Assert.assertTrue(entry.containsKey(KafkaProduceRateTracker.KAFKA_PARTITION_PRODUCE_RATE_KEY));
    }
  }

  static class TestDataPublisher extends DataPublisher {
    public TestDataPublisher(WorkUnitState state) {
      super(state);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) {

    }

    @Override
    public void publishMetadata(Collection<? extends WorkUnitState> states) {

    }

    @Override
    public void close()
        throws IOException {

    }
  }
}