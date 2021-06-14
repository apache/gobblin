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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.LocalDate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.FlushingExtractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.writer.LastWatermarkTracker;
import org.apache.gobblin.writer.WatermarkTracker;


public class KafkaProduceRateTrackerTest {
  private static final LocalDate HOLIDAY_DATE = new LocalDate(2019, 12, 25);
  private static final LocalDate NON_HOLIDAY_DATE = new LocalDate(2020, 01, 05);
  private static final Long HOLIDAY_TIME = HOLIDAY_DATE.toDateTimeAtStartOfDay().toInstant().getMillis();
  private static final Long NON_HOLIDAY_TIME = NON_HOLIDAY_DATE.toDateTimeAtStartOfDay().toInstant().getMillis();

  private KafkaProduceRateTracker tracker;
  private List<KafkaPartition> kafkaPartitions = new ArrayList<>();
  private WatermarkTracker watermarkTracker;
  private WorkUnitState workUnitState;
  private KafkaExtractorStatsTracker extractorStatsTracker;

  @BeforeClass
  public void setUp() {
    kafkaPartitions.add(new KafkaPartition.Builder().withTopicName("test-topic").withId(0).build());
    kafkaPartitions.add(new KafkaPartition.Builder().withTopicName("test-topic").withId(1).build());
    this.workUnitState = new WorkUnitState();
    this.workUnitState.setProp(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY, 5L);
    this.watermarkTracker = new LastWatermarkTracker(false);
    this.extractorStatsTracker = new KafkaExtractorStatsTracker(this.workUnitState, kafkaPartitions);
  }

  private void assertTopicPartitionOrder(KafkaProduceRateTracker tracker, List<KafkaPartition> partitions) {
    Iterator<KafkaPartition> keyIterator = tracker.getPartitionsToProdRate().keySet().iterator();
    for (KafkaPartition partition : partitions) {
      Assert.assertEquals(partition, keyIterator.next());
    }
  }

  private void writeProduceRateToKafkaWatermarksHelper(long readStartTime, long decodeStartTime, long currentTime) {
    this.extractorStatsTracker.reset();
    assertTopicPartitionOrder(tracker, kafkaPartitions);
    extractorStatsTracker.onDecodeableRecord(0, readStartTime, decodeStartTime, 100, currentTime-8000, currentTime-10000);
    readStartTime++;
    decodeStartTime++;
    extractorStatsTracker.onDecodeableRecord(1, readStartTime, decodeStartTime, 200, currentTime-7000, currentTime-9000);
    extractorStatsTracker.updateStatisticsForCurrentPartition(0, readStartTime, currentTime - 8000);
    extractorStatsTracker.updateStatisticsForCurrentPartition(1, readStartTime, currentTime - 7000);

    MultiLongWatermark highWatermark = new MultiLongWatermark(Lists.newArrayList(20L, 30L));
    Map<KafkaPartition, Long> latestOffsetMap = Maps.newHashMap();
    latestOffsetMap.put(kafkaPartitions.get(0), 35L);
    latestOffsetMap.put(kafkaPartitions.get(1), 47L);
    Map<String, CheckpointableWatermark> lastCommittedWatermarks = Maps.newHashMap();

    KafkaPartition topicPartition1 = kafkaPartitions.get(0);
    KafkaPartition topicPartition2 = kafkaPartitions.get(1);

    lastCommittedWatermarks.put(topicPartition1.toString(),
        new KafkaStreamingExtractor.KafkaWatermark(topicPartition1, new LongWatermark(5L)));
    lastCommittedWatermarks.put(topicPartition2.toString(),
        new KafkaStreamingExtractor.KafkaWatermark(topicPartition2, new LongWatermark(7L)));
    this.tracker.writeProduceRateToKafkaWatermarks(latestOffsetMap, lastCommittedWatermarks, highWatermark, currentTime);
  }

  private void assertKafkaWatermarks(long currentTime) {
    Map<String, CheckpointableWatermark> unacknowledgedWatermarks = watermarkTracker.getAllUnacknowledgedWatermarks();
    Assert.assertEquals(unacknowledgedWatermarks.size(), 2);

    KafkaPartition topicPartition1 = kafkaPartitions.get(0);
    KafkaPartition topicPartition2 = kafkaPartitions.get(1);

    for (KafkaPartition topicPartition : Lists.newArrayList(topicPartition1, topicPartition2)) {
      KafkaStreamingExtractor.KafkaWatermark kafkaWatermark = (KafkaStreamingExtractor.KafkaWatermark) unacknowledgedWatermarks.get(topicPartition.toString());
      if (currentTime == HOLIDAY_TIME + 10) {
        Assert.assertTrue(kafkaWatermark.avgProduceRates == null);
        continue;
      }

      Assert.assertTrue(kafkaWatermark.avgProduceRates != null);
      Date date = new Date(currentTime);
      int hourOfDay = KafkaProduceRateTracker.getHourOfDay(date);
      int dayOfWeek = KafkaProduceRateTracker.getDayOfWeek(date);

      Assert.assertTrue(kafkaWatermark.avgProduceRates[dayOfWeek][hourOfDay] > 0);
      for (int i = 0; i < 7; i++) {
        for (int j = 0; j < 24; j++) {
          if (i != dayOfWeek || j != hourOfDay) {
            Assert.assertTrue(kafkaWatermark.avgProduceRates[i][j] < 0);
          }
        }
      }
      Assert.assertTrue(kafkaWatermark.getAvgConsumeRate() > 0);
    }
  }

  @Test
  public void testWriteProduceRateToKafkaWatermarksNoData() {
    long currentTime = System.currentTimeMillis();

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(KafkaProduceRateTracker.KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY, false);
    workUnitState.setProp(FlushingExtractor.FLUSH_INTERVAL_SECONDS_KEY, 1L);
    workUnitState.setProp(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY, 5L);
    WatermarkTracker watermarkTracker = new LastWatermarkTracker(false);
    KafkaExtractorStatsTracker extractorStatsTracker = new KafkaExtractorStatsTracker(workUnitState, kafkaPartitions);

    KafkaProduceRateTracker tracker =
        new KafkaProduceRateTracker(workUnitState, kafkaPartitions, watermarkTracker, extractorStatsTracker, currentTime);

    Map<KafkaPartition, Long> latestOffsetMap = Maps.newHashMap();
    latestOffsetMap.put(kafkaPartitions.get(0), 20L);
    latestOffsetMap.put(kafkaPartitions.get(1), 30L);
    Map<String, CheckpointableWatermark> lastCommittedWatermarks = Maps.newHashMap();

    //No new data; High watermark same as latest offsets
    MultiLongWatermark highWatermark = new MultiLongWatermark(Lists.newArrayList(20L, 30L));
    extractorStatsTracker.reset();
    tracker.writeProduceRateToKafkaWatermarks(latestOffsetMap, lastCommittedWatermarks, highWatermark, currentTime);

    Map<String, CheckpointableWatermark> unacknowledgedWatermarks = watermarkTracker.getAllUnacknowledgedWatermarks();
    for (KafkaPartition topicPartition : kafkaPartitions) {
      KafkaStreamingExtractor.KafkaWatermark kafkaWatermark = (KafkaStreamingExtractor.KafkaWatermark) unacknowledgedWatermarks.get(topicPartition.toString());
      Assert.assertTrue(kafkaWatermark.avgProduceRates != null);
      Assert.assertTrue(kafkaWatermark.avgConsumeRate < 0);
      Assert.assertTrue(kafkaWatermark.getLwm().getValue() > 0);
    }
  }

  @Test (dependsOnMethods = "testWriteProduceRateToKafkaWatermarksNoData")
  public void testWriteProduceRateToKafkaWatermarks() {
    long readStartTime = System.nanoTime();
    long decodeStartTime = readStartTime + 1;
    long currentTime = System.currentTimeMillis();

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(KafkaProduceRateTracker.KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY, false);
    workUnitState.setProp(FlushingExtractor.FLUSH_INTERVAL_SECONDS_KEY, 1L);
    this.tracker = new KafkaProduceRateTracker(workUnitState, kafkaPartitions, watermarkTracker, extractorStatsTracker, currentTime);

    //Bootstrap the extractorStatsTracker
    writeProduceRateToKafkaWatermarksHelper(readStartTime, decodeStartTime, currentTime);

    for (int i = 1; i < KafkaProduceRateTracker.SLIDING_WINDOW_SIZE + 1; i++) {
      //Add more records and update watermarks/stats
      writeProduceRateToKafkaWatermarksHelper(readStartTime + 1000 + i, decodeStartTime + 1000 + i,
          currentTime + i);
    }

    //Ensure kafka watermark is non-null and is > 0 for the hour-of-day and day-of-week corresponding to currentTime
    assertKafkaWatermarks(currentTime + KafkaProduceRateTracker.SLIDING_WINDOW_SIZE);
  }

  @Test (dependsOnMethods = "testWriteProduceRateToKafkaWatermarks")
  public void testWriteProduceRateToKafkaWatermarksWithHolidays() {
    long readStartTime = TimeUnit.MILLISECONDS.toNanos(HOLIDAY_TIME);
    long decodeStartTime = readStartTime + 1;
    Long currentTime = HOLIDAY_TIME + 10;

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(KafkaProduceRateTracker.KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY, true);
    workUnitState.setProp(FlushingExtractor.FLUSH_INTERVAL_SECONDS_KEY, 1L);
    this.tracker = new KafkaProduceRateTracker(workUnitState, kafkaPartitions, watermarkTracker, extractorStatsTracker, currentTime);

    //Bootstrap the extractorStatsTracker; Holiday stats collection disabled.
    writeProduceRateToKafkaWatermarksHelper(readStartTime, decodeStartTime, currentTime);

    //Add a more records and update watermarks/stats
    writeProduceRateToKafkaWatermarksHelper(readStartTime + 1000, decodeStartTime + 1000, currentTime + 1);

    //Since stats collection is disabled on holidays, ensure watermarks are null.
    assertKafkaWatermarks(currentTime);

    readStartTime = TimeUnit.MILLISECONDS.toNanos(NON_HOLIDAY_TIME);
    decodeStartTime = readStartTime + 1;
    currentTime = NON_HOLIDAY_TIME + 10;

    //Bootstrap the extractorStatsTracker with initial records
    writeProduceRateToKafkaWatermarksHelper(readStartTime, decodeStartTime, currentTime);

    for (int i = 1; i < KafkaProduceRateTracker.SLIDING_WINDOW_SIZE + 1; i++) {
      //Add more records and update watermarks/stats
      writeProduceRateToKafkaWatermarksHelper(readStartTime + 1000 + i, decodeStartTime + 1000 + i,
          currentTime + i);
    }
    //Ensure kafka watermark is not null and is > 0 for the hour-of-day and day-of-week corresponding to currentTime
    assertKafkaWatermarks(currentTime + KafkaProduceRateTracker.SLIDING_WINDOW_SIZE);
  }

  @Test
  public void testIsHoliday() {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(KafkaProduceRateTracker.KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY, true);
    KafkaExtractorStatsTracker extractorStatsTracker = new KafkaExtractorStatsTracker(this.workUnitState, kafkaPartitions);
    KafkaProduceRateTracker tracker = new KafkaProduceRateTracker(workUnitState, kafkaPartitions, watermarkTracker, extractorStatsTracker);
    Assert.assertTrue(tracker.isHoliday(HOLIDAY_DATE));
    //Ensure that the caching behavior is correct
    Assert.assertTrue(tracker.isHoliday(HOLIDAY_DATE));
    Assert.assertFalse(tracker.isHoliday(NON_HOLIDAY_DATE));
  }

  @Test
  public void testGetPenultimateElement() {
    EvictingQueue<Double> queue = EvictingQueue.create(3);

    queue.add(1.0);
    queue.add(2.0);
    queue.add(3.0);

    Double element = KafkaProduceRateTracker.getPenultimateElement(queue);
    Assert.assertEquals(element, 2.0);

    queue.add(4.0);
    element = KafkaProduceRateTracker.getPenultimateElement(queue);
    Assert.assertEquals(element, 3.0);

    queue.add(5.0);
    element = KafkaProduceRateTracker.getPenultimateElement(queue);
    Assert.assertEquals(element, 4.0);
  }
}