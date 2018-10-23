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

package org.apache.gobblin.source.extractor.watermark;

import org.apache.gobblin.source.extractor.extract.QueryBasedExtractor;
import org.apache.gobblin.source.extractor.utils.Utils;

import java.math.RoundingMode;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;


public class SimpleWatermark implements Watermark {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleWatermark.class);
  private static final int deltaForNextWatermark = 1;
  private String watermarkColumn;

  public SimpleWatermark(String watermarkColumn, String watermarkFormat) {
    this.watermarkColumn = watermarkColumn;
  }

  @Override
  public String getWatermarkCondition(QueryBasedExtractor<?, ?> extractor, long watermarkValue, String operator) {
    return Utils.getCoalesceColumnNames(this.watermarkColumn) + " " + operator + " " + watermarkValue;
  }

  @Override
  public int getDeltaNumForNextWatermark() {
    return deltaForNextWatermark;
  }

  @Override
  public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, long partitionInterval,
      int maxIntervals) {
    Preconditions.checkArgument(partitionInterval >= 1,
        "Invalid value for partitionInterval, value should be at least 1.");
    Preconditions.checkArgument(maxIntervals > 0, "Invalid value for maxIntervals, positive value expected.");

    HashMap<Long, Long> intervalMap = new HashMap<>();
    long nextNum;
    long interval = getInterval(lowWatermarkValue, highWatermarkValue, partitionInterval, maxIntervals);
    LOG.info("Recalculated partition interval:" + interval);
    if (interval == 0) {
      return intervalMap;
    }

    long startNum = lowWatermarkValue;
    long endNum = highWatermarkValue;
    boolean longOverflow = false;

    if (startNum == endNum) {
      intervalMap.put(startNum, endNum);
      return intervalMap;
    }

    while (startNum < endNum && !longOverflow) {
      longOverflow = (Long.MAX_VALUE - interval < startNum);
      nextNum = longOverflow ? endNum : Math.min(startNum + interval, endNum);
      intervalMap.put(startNum, nextNum);
      startNum = nextNum;
    }
    return intervalMap;
  }

  /**
   * recalculate interval if total number of partitions greater than maximum number of allowed partitions
   *
   * @param lowWatermarkValue low watermark value
   * @param highWatermarkValue high watermark value
   * @param partitionInterval partition interval
   * @param maxIntervals max number of allowed partitions
   * @return calculated interval
   */
  private static long getInterval(long lowWatermarkValue, long highWatermarkValue, long partitionInterval,
      int maxIntervals) {
    if (lowWatermarkValue > highWatermarkValue) {
      LOG.info(
          "lowWatermarkValue: " + lowWatermarkValue + " is greater than highWatermarkValue: " + highWatermarkValue);

      return 0;
    }
    long outputInterval = partitionInterval;
    boolean longOverflow = false;
    long totalIntervals = Long.MAX_VALUE;
    try {
      totalIntervals = DoubleMath.roundToLong(
          (double) highWatermarkValue / partitionInterval - (double) lowWatermarkValue / partitionInterval,
          RoundingMode.CEILING);
    } catch (java.lang.ArithmeticException e) {
      longOverflow = true;
    }
    if (longOverflow || totalIntervals > maxIntervals) {
      outputInterval = DoubleMath.roundToLong(
          (double) highWatermarkValue / maxIntervals - (double) lowWatermarkValue / maxIntervals, RoundingMode.CEILING);

    }
    return outputInterval;
  }

  /**
   * Adjust the given watermark by diff
   *
   * @param baseWatermark the original watermark
   * @param diff the amount to change
   * @return the adjusted watermark value
   */
  public static long adjustWatermark(String baseWatermark, int diff) {
    return Long.parseLong(baseWatermark) + diff;
  }
}
