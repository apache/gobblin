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
package org.apache.gobblin.source.extractor.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.ExtractType;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;

/**
 * Unit tests for {@link PartitionerTest}
 */
public class PartitionerTest {
  @Test
  public void testGetPartitionList() {
    List<Partition> expectedPartitions = new ArrayList<>();
    SourceState sourceState = new SourceState();
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE, true);

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    long defaultValue = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    expectedPartitions.add(new Partition(defaultValue, defaultValue, true, false));

    // Watermark doesn't exist
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Set watermark
    sourceState.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, "time");
    // Set other properties
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "hour");
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "SNAPSHOT");
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_PARTITION_INTERVAL, "2");
    sourceState.setProp(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, "2");
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE, true);

    expectedPartitions.clear();
    expectedPartitions.add(new Partition(defaultValue, Long.parseLong(TestPartitioner.currentTimeString), true, false));
    // No user specified watermarks
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Set user specified low and high watermarks
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE, "20170101002010");
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, "20170101122010");

    expectedPartitions.clear();
    expectedPartitions.add(new Partition(20170101000000L, 20170101060000L));
    expectedPartitions.add(new Partition(20170101060000L, 20170101120000L, true, true));
    List<Partition> partitions = partitioner.getPartitionList(-1);
    Collections.sort(partitions, Partitioner.ascendingComparator);
    Assert.assertEquals(partitions, expectedPartitions);
  }

  @Test
  public void testGetUserSpecifiedPartitionList() {
    List<Partition> expectedPartitions = new ArrayList<>();
    SourceState sourceState = new SourceState();
    sourceState.setProp(Partitioner.HAS_USER_SPECIFIED_PARTITIONS, true);

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    long defaultValue = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    expectedPartitions.add(new Partition(defaultValue, defaultValue, true, true));
    sourceState.setProp(Partitioner.USER_SPECIFIED_PARTITIONS, "");
    // Partition list doesn't exist
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Date partitions
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "date");

    // Only one partition point
    sourceState.setProp(Partitioner.USER_SPECIFIED_PARTITIONS, "20140101030201");
    expectedPartitions.clear();
    expectedPartitions.add(new Partition(20140101000000L, 20170101000000L, true, false));
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Keep upper bounds for append_daily job
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "APPEND_DAILY");
    sourceState.setProp(Partitioner.USER_SPECIFIED_PARTITIONS, "20140101030201, 20140102040201");
    expectedPartitions.clear();
    expectedPartitions.add(new Partition(20140101000000L, 20140102000000L, true, true));
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Hour partitions, snapshot extract
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "hour");
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "SNAPSHOT");
    expectedPartitions.clear();
    expectedPartitions.add(new Partition(20140101030000L, 20140102040000L,  true, false));
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);

    // Hour partitions, timestamp extract
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "timestamp");
    expectedPartitions.clear();
    expectedPartitions.add(new Partition(20140101030201L, 20140102040201L, true,false));
    Assert.assertEquals(partitioner.getPartitionList(-1), expectedPartitions);
  }

  /**
   * Test getLowWatermark. Is watermark override: true.
   */
  @Test
  public void testGetLowWatermarkOnUserOverride() {
    String startValue = "20140101000000";
    SourceState sourceState = new SourceState();
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE, true);
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE, startValue);

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    Assert.assertEquals(
        partitioner.getLowWatermark(null, null, -1, 0),
        Long.parseLong(startValue),
        "Low watermark should be " + startValue);

    // It works for full dump too
    sourceState.removeProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE);
    sourceState.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
    Assert.assertEquals(
        partitioner.getLowWatermark(null, null, -1, 0),
        Long.parseLong(startValue),
        "Low watermark should be " + startValue);

    // Should return ConfigurationKeys.DEFAULT_WATERMARK_VALUE if no SOURCE_QUERYBASED_START_VALUE is specified
    sourceState.removeProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE);
    Assert.assertEquals(
        partitioner.getLowWatermark(null, null, -1, 0),
        ConfigurationKeys.DEFAULT_WATERMARK_VALUE,
        "Low watermark should be " + ConfigurationKeys.DEFAULT_WATERMARK_VALUE);
  }


  /**
   * Test getLowWatermark. Extract type: Snapshot.
   */
  @Test
  public void testGetLowWatermarkOnSnapshotExtract() {
    SourceState sourceState = new SourceState();
    String startValue = "20140101000000";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE, startValue);
    TestPartitioner partitioner = new TestPartitioner(sourceState);

    ExtractType extractType = ExtractType.SNAPSHOT;
    int delta = 1;

    // No previous watermark
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, null, ConfigurationKeys.DEFAULT_WATERMARK_VALUE, delta),
        Long.parseLong(startValue),
        "Low watermark should be " + startValue);

    // With previous watermark
    long previousWatermark = 20140101000050L;
    long expected = previousWatermark + delta;
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.SIMPLE, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.TIMESTAMP, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    // With SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS
    int backupSecs = 10;
    expected = previousWatermark + delta - backupSecs;
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, backupSecs);
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.SIMPLE, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.TIMESTAMP, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);
  }

  /**
   * Test getLowWatermark. Extract type: Append.
   */
  @Test
  public void testGetLowWatermarkOnAppendExtract() {
    SourceState sourceState = new SourceState();
    String startValue = "20140101000000";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE, startValue);
    TestPartitioner partitioner = new TestPartitioner(sourceState);

    ExtractType extractType = ExtractType.APPEND_DAILY;
    int delta = 1;

    // No previous watermark
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, null, ConfigurationKeys.DEFAULT_WATERMARK_VALUE, delta),
        Long.parseLong(startValue),
        "Low watermark should be " + startValue);

    // With previous watermark
    long previousWatermark = 20140101000050L;
    long expected = previousWatermark + delta;
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.SIMPLE, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.TIMESTAMP, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    // The result has nothing to do with SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS
    int backupSecs = 10;
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, backupSecs);
    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.SIMPLE, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getLowWatermark(extractType, WatermarkType.TIMESTAMP, previousWatermark, delta),
        expected,
        "Low watermark should be " + expected);
  }

  /**
   * Test getHighWatermark. Is watermark override: true.
   */
  @Test
  public void testGetHighWatermarkOnUserOverride() {
    String endValue = "20140101000000";
    SourceState sourceState = new SourceState();
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE, true);
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, endValue);

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    Assert.assertEquals(
        partitioner.getHighWatermark(null, null),
        Long.parseLong(endValue),
        "High watermark should be " + endValue);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should mark as user specified high watermark");

    partitioner.reset();

    // Should return current time if no SOURCE_QUERYBASED_END_VALUE is specified
    sourceState.removeProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE);
    long expected = Long.parseLong(TestPartitioner.currentTimeString);
    Assert.assertEquals(
        partitioner.getHighWatermark(null, null),
        expected,
        "High watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        false,
        "Should not mark as user specified high watermark");
  }

  /**
   * Test getHighWatermark. Extract type: Snapshot.
   */
  @Test
  public void testGetHighWatermarkOnSnapshotExtract() {
    String endValue = "20140101000000";
    SourceState sourceState = new SourceState();
    // It won't use SOURCE_QUERYBASED_END_VALUE when extract is full
    sourceState.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, endValue);

    ExtractType extractType = ExtractType.SNAPSHOT;

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, WatermarkType.SIMPLE),
        ConfigurationKeys.DEFAULT_WATERMARK_VALUE,
        "High watermark should be " + ConfigurationKeys.DEFAULT_WATERMARK_VALUE);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        false,
        "Should not mark as user specified high watermark");

    long expected = Long.parseLong(TestPartitioner.currentTimeString);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, WatermarkType.TIMESTAMP),
        expected,
        "High watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        false,
        "Should not mark as user specified high watermark");
  }

  /**
   * Test getHighWatermark. Extract type: Append.
   */
  @Test
  public void testGetHighWatermarkOnAppendExtract() {
    String endValue = "20140101000000";
    SourceState sourceState = new SourceState();
    sourceState.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, endValue);

    ExtractType extractType = ExtractType.APPEND_DAILY;

    TestPartitioner partitioner = new TestPartitioner(sourceState);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        Long.parseLong(endValue),
        "High watermark should be " + endValue);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should mark as user specified high watermark");

    partitioner.reset();

    // Test non-full-dump cases below
    sourceState.removeProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY);

    // No limit type
    Assert.assertEquals(
        partitioner.getHighWatermark(ExtractType.APPEND_BATCH, null),
        ConfigurationKeys.DEFAULT_WATERMARK_VALUE,
        "High watermark should be " + ConfigurationKeys.DEFAULT_WATERMARK_VALUE);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        false,
        "Should not mark as user specified high watermark");

    // No limit delta
    long expected = Long.parseLong(TestPartitioner.currentTimeString);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        expected,
        "High watermark should be " + expected);

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        false,
        "Should not mark as user specified high watermark");

    // CURRENTDATE - 1
    String maxLimit = "CURRENTDATE-1";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT, maxLimit);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        20161231235959L,
        "High watermark should be 20161231235959");

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should not mark as user specified high watermark");

    partitioner.reset();

    // CURRENTHOUR - 1
    maxLimit = "CURRENTHOUR-1";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT, maxLimit);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        20161231235959L,
        "High watermark should be 20161231235959");

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should not mark as user specified high watermark");

    partitioner.reset();

    // CURRENTMINUTE - 1
    maxLimit = "CURRENTMINUTE-1";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT, maxLimit);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        20161231235959L,
        "High watermark should be 20161231235959");

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should not mark as user specified high watermark");

    partitioner.reset();

    // CURRENTSECOND - 1
    maxLimit = "CURRENTSECOND-1";
    sourceState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT, maxLimit);
    Assert.assertEquals(
        partitioner.getHighWatermark(extractType, null),
        20161231235959L,
        "High watermark should be 20161231235959");

    Assert.assertEquals(
        partitioner.getUserSpecifiedHighWatermark(),
        true,
        "Should not mark as user specified high watermark");
  }

  private class TestPartitioner extends Partitioner {
    static final String currentTimeString = "20170101000000";

    private DateTime currentTime;
    TestPartitioner(SourceState state) {
      super(state);
      currentTime = Utils.toDateTime(currentTimeString, "yyyyMMddHHmmss", ConfigurationKeys.DEFAULT_SOURCE_TIMEZONE);
    }

    boolean getUserSpecifiedHighWatermark() {
      return hasUserSpecifiedHighWatermark;
    }

    @Override
    public DateTime getCurrentTime(String timeZone) {
      return currentTime;
    }

    void reset() {
      hasUserSpecifiedHighWatermark = false;
    }
  }

}
