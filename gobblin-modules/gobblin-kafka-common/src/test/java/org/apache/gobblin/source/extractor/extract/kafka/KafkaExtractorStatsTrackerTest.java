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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.configuration.WorkUnitState;


public class KafkaExtractorStatsTrackerTest {
  List<KafkaPartition> kafkaPartitions = new ArrayList<>();
  private KafkaExtractorStatsTracker extractorStatsTracker;
  final static KafkaPartition PARTITION0 =  new KafkaPartition.Builder().withTopicName("test-topic").withId(0).build();
  final static KafkaPartition PARTITION1 =  new KafkaPartition.Builder().withTopicName("test-topic").withId(1).build();

  @BeforeClass
  public void setUp() {
    kafkaPartitions.add(PARTITION0);
    kafkaPartitions.add(PARTITION1);
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY, 10L);
    workUnitState.setProp(KafkaSource.OBSERVED_LAG_MEASUREMENT_ENABLED, true);
    this.extractorStatsTracker = new KafkaExtractorStatsTracker(workUnitState, kafkaPartitions);
  }

  @Test
  public void testOnUndecodeableRecord() {
    //Ensure that error counters are initialized correctly
    Assert.assertEquals(this.extractorStatsTracker.getErrorPartitionCount(), 0);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(0).longValue(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(0).longValue(), -1);

    //Ensure that error counters are updated correctly after 1st call to KafkaExtractorStatsTracker#onUndecodeableRecord()
    this.extractorStatsTracker.onUndecodeableRecord(0);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(0).longValue(), 1);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(1).longValue(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getErrorPartitionCount(), 1);

    //Ensure that error counters are updated correctly after 2nd call to KafkaExtractorStatsTracker#onUndecodeableRecord()
    this.extractorStatsTracker.onUndecodeableRecord(0);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(0).longValue(), 2);
    Assert.assertEquals(this.extractorStatsTracker.getDecodingErrorCount(1).longValue(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getErrorPartitionCount(), 1);
  }

  @Test
  public void testResetStartFetchEpochTime() {
    long currentTime = System.currentTimeMillis();
    this.extractorStatsTracker.resetStartFetchEpochTime(1);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getStartFetchEpochTime() >= currentTime);
  }

  @Test
  public void testOnDecodeableRecord() throws InterruptedException {
    long readStartTime = System.nanoTime();
    Thread.sleep(1);
    long decodeStartTime = System.nanoTime();
    long currentTimeMillis = System.currentTimeMillis();
    long logAppendTimestamp = currentTimeMillis - 15 * 60 * 1000L;
    long recordCreationTimestamp = currentTimeMillis - 16 * 60 * 1000L;
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getProcessedRecordCount(), 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getPartitionTotalSize(), 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getDecodeRecordTime() == 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getReadRecordTime() == 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getSlaMissedRecordCount(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMinLogAppendTime(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMaxLogAppendTime(), -1);
    Assert.assertEquals(this.extractorStatsTracker.getObservedLagHistogram().getTotalCount(), 0);

    this.extractorStatsTracker.onDecodeableRecord(0, readStartTime, decodeStartTime, 100, logAppendTimestamp, recordCreationTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getProcessedRecordCount(), 1);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getPartitionTotalSize(), 100);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getDecodeRecordTime() > 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getReadRecordTime() > 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getSlaMissedRecordCount(), 1);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMinLogAppendTime(), logAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMaxLogAppendTime(), logAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getObservedLagHistogram().getTotalCount(), 1);

    readStartTime = System.nanoTime();
    Thread.sleep(1);
    decodeStartTime = System.nanoTime();
    long previousLogAppendTimestamp = logAppendTimestamp;
    currentTimeMillis = System.currentTimeMillis();
    logAppendTimestamp = currentTimeMillis - 10;
    recordCreationTimestamp = currentTimeMillis - 20;
    long previousDecodeRecordTime = this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getDecodeRecordTime();
    long previousReadRecordTime = this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getReadRecordTime();

    this.extractorStatsTracker.onDecodeableRecord(0, readStartTime, decodeStartTime, 100, logAppendTimestamp, recordCreationTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getProcessedRecordCount(), 2);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getPartitionTotalSize(), 200);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getDecodeRecordTime() > previousDecodeRecordTime);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getReadRecordTime() > previousReadRecordTime);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getSlaMissedRecordCount(), 1);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMinLogAppendTime(), previousLogAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getMaxLogAppendTime(), logAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getObservedLagHistogram().getTotalCount(), 2);
  }

  @Test
  public void testOnFetchNextMessageBuffer() throws InterruptedException {
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getFetchMessageBufferTime(), 0);
    long fetchStartTime = System.nanoTime();
    Thread.sleep(1);
    this.extractorStatsTracker.onFetchNextMessageBuffer(1, fetchStartTime);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getFetchMessageBufferTime() > 0);
  }

  @Test
  public void testOnPartitionReadComplete() throws InterruptedException {
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getReadRecordTime(), 0);
    long readStartTime = System.nanoTime();
    Thread.sleep(1);
    this.extractorStatsTracker.onPartitionReadComplete(1, readStartTime);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getReadRecordTime() > 0);
  }

  @Test (dependsOnMethods = "testOnDecodeableRecord")
  public void testUpdateStatisticsForCurrentPartition()
      throws InterruptedException {
    long readStartTime = System.nanoTime();
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getStopFetchEpochTime(), 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getElapsedTime(), 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getAvgMillisPerRecord() < 0);
    this.extractorStatsTracker.updateStatisticsForCurrentPartition(0, readStartTime, 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getStopFetchEpochTime() > 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getElapsedTime() > 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getAvgMillisPerRecord() > 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(0)).getAvgRecordSize(), 100);

    readStartTime = System.nanoTime();
    long currentTimeMillis = System.currentTimeMillis();
    long logAppendTimestamp = currentTimeMillis - 10;
    long recordCreationTimestamp = currentTimeMillis - 20;
    Thread.sleep(1);
    long decodeStartTime = System.nanoTime();
    this.extractorStatsTracker.onDecodeableRecord(1, readStartTime, decodeStartTime, 100, logAppendTimestamp, recordCreationTimestamp);
    this.extractorStatsTracker.updateStatisticsForCurrentPartition(1, readStartTime, 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getElapsedTime() > 0);
    Assert.assertTrue(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getAvgMillisPerRecord() > 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getAvgRecordSize(), 100);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getSlaMissedRecordCount(), 0);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getMinLogAppendTime(), logAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getStatsMap().get(kafkaPartitions.get(1)).getMaxLogAppendTime(), logAppendTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getObservedLagHistogram().getTotalCount(), 3);
  }

  @Test (dependsOnMethods = "testUpdateStatisticsForCurrentPartition")
  public void testGetAvgRecordSize() {
    Assert.assertEquals(this.extractorStatsTracker.getAvgRecordSize(0), 100);
    Assert.assertEquals(this.extractorStatsTracker.getAvgRecordSize(1), 100);
    this.extractorStatsTracker.reset();
    Assert.assertEquals(this.extractorStatsTracker.getAvgRecordSize(0), 0);
    long readStartTime = System.nanoTime();
    long decodeStartTime = readStartTime + 1;
    long currentTimeMillis = System.currentTimeMillis();
    long logAppendTimestamp = currentTimeMillis - 10;
    long recordCreationTimestamp = currentTimeMillis - 20;
    this.extractorStatsTracker.onDecodeableRecord(1, readStartTime, decodeStartTime, 150, logAppendTimestamp, recordCreationTimestamp);
    Assert.assertEquals(this.extractorStatsTracker.getAvgRecordSize(1), 150);
  }

  @Test
  public void testGenerateTagsForPartitions() throws Exception {
    MultiLongWatermark lowWatermark = new MultiLongWatermark(Arrays.asList(new Long(10), new Long(20)));
    MultiLongWatermark highWatermark = new MultiLongWatermark(Arrays.asList(new Long(20), new Long(30)));
    MultiLongWatermark nextWatermark = new MultiLongWatermark(Arrays.asList(new Long(15), new Long(25)));
    Map<KafkaPartition, Map<String, String>> addtionalTags =
        ImmutableMap.of(PARTITION0, ImmutableMap.of("testKey", "testValue"));
    Map<KafkaPartition, Map<String, String>> result =
        extractorStatsTracker.generateTagsForPartitions(lowWatermark, highWatermark, nextWatermark, addtionalTags);
    Assert.assertTrue(result.get(PARTITION0).containsKey("testKey"));
    Assert.assertEquals(result.get(PARTITION0).get("testKey"), "testValue");
    Assert.assertFalse(result.get(PARTITION1).containsKey("testKey"));
  }

  @Test
  public void testConvertHistogramToString() {
    Histogram histogram = new Histogram(1, 100, 3);
    histogram.recordValue(3);
    histogram.recordValue(25);
    histogram.recordValue(25);
    histogram.recordValue(92);
    String histogramString = KafkaExtractorStatsTracker.convertHistogramToString(histogram);

    HistogramLogReader logReader = new HistogramLogReader(new ByteArrayInputStream(histogramString.getBytes(
        Charsets.UTF_8)));
    Histogram histogram1 = (Histogram) logReader.nextIntervalHistogram();
    Assert.assertEquals(histogram1.getTotalCount(), 4);
    Assert.assertEquals(histogram1.getMaxValue(), 92);
    Assert.assertEquals(histogram1.getCountAtValue(25), 2);
    Assert.assertEquals(histogram1.getCountAtValue(3), 1);
    Assert.assertEquals(histogram1.getCountAtValue(92), 1);
  }
}