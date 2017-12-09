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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * This class encapsulates the two ends, {@link #lowWatermark} and {@link #highWatermark}, of a partition and some
 * metadata, e.g. {@link #hasUserSpecifiedHighWatermark}, to describe the partition.
 *
 * <p>
 *   A {@link Source} partitions its data into a collection of {@link Partition}, each of which will be used to create
 *   a {@link WorkUnit}.
 *
 *   Currently, the {@link #lowWatermark} of a partition in Gobblin is inclusive and its {@link #highWatermark} is
 *   exclusive unless it is the last partition.
 * </p>
 *
 * @author zhchen
 */
@EqualsAndHashCode
public class Partition {
  public static final String IS_LAST_PARTIITON = "partition.isLastPartition";
  public static final String HAS_USER_SPECIFIED_HIGH_WATERMARK = "partition.hasUserSpecifiedHighWatermark";

  @Getter
  private final long lowWatermark;
  @Getter
  private final boolean isLowWatermarkInclusive;
  @Getter
  private final long highWatermark;
  @Getter
  private final boolean isHighWatermarkInclusive;
  @Getter
  private final boolean isLastPartition;

  /**
   * Indicate if the Partition highWatermark is set as user specifies, not computed on the fly
   */
  private final boolean hasUserSpecifiedHighWatermark;

  public Partition(long lowWatermark, long highWatermark, boolean isLastPartition,
      boolean hasUserSpecifiedHighWatermark) {
    this.lowWatermark = lowWatermark;
    this.highWatermark = highWatermark;

    this.isLowWatermarkInclusive = true;
    this.isHighWatermarkInclusive = isLastPartition;

    this.isLastPartition = isLastPartition;
    this.hasUserSpecifiedHighWatermark = hasUserSpecifiedHighWatermark;
  }

  public Partition(long lowWatermark, long highWatermark, boolean hasUserSpecifiedHighWatermark) {
    this(lowWatermark, highWatermark, false, hasUserSpecifiedHighWatermark);
  }

  public Partition(long lowWatermark, long highWatermark) {
    this(lowWatermark, highWatermark, false);
  }

  public boolean getHasUserSpecifiedHighWatermark() {
    return hasUserSpecifiedHighWatermark;
  }

  public void serialize(WorkUnit workUnit) {
    workUnit.setWatermarkInterval(
        new WatermarkInterval(new LongWatermark(lowWatermark), new LongWatermark(highWatermark)));
    if (hasUserSpecifiedHighWatermark) {
      workUnit.setProp(Partition.HAS_USER_SPECIFIED_HIGH_WATERMARK, true);
    }
    if (isLastPartition) {
      workUnit.setProp(Partition.IS_LAST_PARTIITON, true);
    }
  }

  public static Partition deserialize(WorkUnit workUnit) {
    long lowWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    long highWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

    if (workUnit.getProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY) != null) {
      lowWatermark = workUnit.getLowWatermark(LongWatermark.class).getValue();
      highWatermark = workUnit.getExpectedHighWatermark(LongWatermark.class).getValue();
    }

    return new Partition(lowWatermark, highWatermark, workUnit.getPropAsBoolean(Partition.IS_LAST_PARTIITON),
        workUnit.getPropAsBoolean(Partition.HAS_USER_SPECIFIED_HIGH_WATERMARK));
  }
}
