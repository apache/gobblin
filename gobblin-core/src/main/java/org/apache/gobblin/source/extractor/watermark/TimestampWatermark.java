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

import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;

import org.apache.gobblin.source.extractor.extract.QueryBasedExtractor;
import org.apache.gobblin.source.extractor.utils.Utils;


public class TimestampWatermark implements Watermark {
  private static final Logger LOG = LoggerFactory.getLogger(TimestampWatermark.class);
  // default water mark format(input format) example: 20140301050505
  private static final String INPUTFORMAT = "yyyyMMddHHmmss";
  private final SimpleDateFormat inputFormatParser;
  private static final int deltaForNextWatermark = 1;
  private String watermarkColumn;
  private String watermarkFormat;

  public TimestampWatermark(String watermarkColumn, String watermarkFormat) {
    this.watermarkColumn = watermarkColumn;
    this.watermarkFormat = watermarkFormat;
    inputFormatParser = new SimpleDateFormat(INPUTFORMAT);
  }

  @Override
  public String getWatermarkCondition(QueryBasedExtractor<?, ?> extractor, long watermarkValue, String operator) {
    return extractor
        .getTimestampPredicateCondition(this.watermarkColumn, watermarkValue, this.watermarkFormat, operator);
  }

  @Override
  public int getDeltaNumForNextWatermark() {
    return deltaForNextWatermark;
  }

  @Override
  synchronized public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue,
      long partitionInterval, int maxIntervals) {
    Preconditions
        .checkArgument(partitionInterval >= 1, "Invalid value for partitionInterval, value should be at least 1.");
    Preconditions.checkArgument(maxIntervals > 0, "Invalid value for maxIntervals, positive value expected.");

    HashMap<Long, Long> intervalMap = new HashMap<>();

    if (lowWatermarkValue > highWatermarkValue) {
      LOG.warn(
          "lowWatermarkValue: " + lowWatermarkValue + " is greater than highWatermarkValue: " + highWatermarkValue);
      return intervalMap;
    }

    final Calendar calendar = Calendar.getInstance();
    Date nextTime;
    final long lowWatermark = toEpoch(Long.toString(lowWatermarkValue));
    final long highWatermark = toEpoch(Long.toString(highWatermarkValue));

    long interval = getInterval(highWatermark - lowWatermark, partitionInterval, maxIntervals);
    LOG.info("Recalculated partition interval:" + interval + " hours");
    if (interval == 0) {
      return intervalMap;
    }

    Date startTime = new Date(lowWatermark);
    Date endTime = new Date(highWatermark);
    LOG.debug("Sart time:" + startTime + "; End time:" + endTime);
    long lwm;
    long hwm;

    if (startTime.getTime() == endTime.getTime()) {
      lwm = Long.parseLong(inputFormatParser.format(startTime));
      hwm = lwm;
      intervalMap.put(lwm, hwm);
      return intervalMap;
    }

    while (startTime.getTime() < endTime.getTime()) {
      lwm = Long.parseLong(inputFormatParser.format(startTime));
      calendar.setTime(startTime);
      calendar.add(Calendar.HOUR, (int) interval);
      nextTime = calendar.getTime();
      hwm = Long.parseLong(inputFormatParser.format(nextTime.getTime() <= endTime.getTime() ? nextTime : endTime));
      intervalMap.put(lwm, hwm);
      LOG.debug("Partition - low:" + lwm + "; high:" + hwm);
      startTime = nextTime;
    }
    return intervalMap;
  }

  /**
   * recalculate interval(in hours) if total number of partitions greater than maximum number of allowed partitions
   *
   * @param diffInMilliSecs difference in range
   * @param hourInterval hour interval (ex: 4 hours)
   * @param maxIntervals max number of allowed partitions
   * @return calculated interval in hours
   */

  private static int getInterval(long diffInMilliSecs, long hourInterval, int maxIntervals) {

    long totalHours = DoubleMath.roundToInt((double) diffInMilliSecs / (60 * 60 * 1000), RoundingMode.CEILING);
    long totalIntervals = DoubleMath.roundToInt((double) totalHours / hourInterval, RoundingMode.CEILING);
    if (totalIntervals > maxIntervals) {
      hourInterval = DoubleMath.roundToInt((double) totalHours / maxIntervals, RoundingMode.CEILING);
    }
    return Ints.checkedCast(hourInterval);
  }

  synchronized private static long toEpoch(String dateTime) {
    Date date = null;
    final SimpleDateFormat inputFormat = new SimpleDateFormat(INPUTFORMAT);
    try {
      date = inputFormat.parse(dateTime);
    } catch (ParseException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return date.getTime();
  }

  /**
   * Adjust the given watermark by diff
   *
   * @param baseWatermark the original watermark
   * @param diff the amount to change
   * @return the adjusted watermark value
   */
  public static long adjustWatermark(String baseWatermark, int diff) {
    SimpleDateFormat parser = new SimpleDateFormat(INPUTFORMAT);
    try {
      Date date = parser.parse(baseWatermark);
      return Long.parseLong(parser.format(Utils.addSecondsToDate(date, diff)));
    } catch (ParseException e) {
      LOG.error("Fail to adjust timestamp watermark", e);
    }
    return -1;
  }
}
