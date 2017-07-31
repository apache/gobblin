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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;


public class HourWatermark implements Watermark {
  private static final Logger LOG = LoggerFactory.getLogger(HourWatermark.class);
  // default water mark format(input format) example: 20140301050505
  private static final String INPUTFORMAT = "yyyyMMddHHmmss";
  // output format of hour water mark example: 2014030105
  private static final String OUTPUTFORMAT = "yyyyMMddHH";
  private static final int deltaForNextWatermark = 60 * 60;
  private final SimpleDateFormat inputFormatParser;
  private String watermarkColumn;
  private String watermarkFormat;

  public HourWatermark(String watermarkColumn, String watermarkFormat) {
    this.watermarkColumn = watermarkColumn;
    this.watermarkFormat = watermarkFormat;
    inputFormatParser = new SimpleDateFormat(INPUTFORMAT);
  }

  @Override
  public String getWatermarkCondition(QueryBasedExtractor<?, ?> extractor, long watermarkValue, String operator) {
    return extractor.getHourPredicateCondition(this.watermarkColumn, watermarkValue, this.watermarkFormat, operator);
  }

  @Override
  public int getDeltaNumForNextWatermark() {
    return deltaForNextWatermark;
  }

  @Override
  synchronized public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue,
      long partitionIntervalInHours, int maxIntervals) {
    Preconditions.checkArgument(maxIntervals > 0, "Invalid value for maxIntervals, positive value expected.");
    Preconditions
        .checkArgument(partitionIntervalInHours > 0, "Invalid value for partitionInterval, should be at least 1.");
    HashMap<Long, Long> intervalMap = Maps.newHashMap();

    if (lowWatermarkValue > highWatermarkValue) {
      LOG.warn("The low water mark is greater than the high water mark, empty intervals are returned");
      return intervalMap;
    }

    final Calendar calendar = Calendar.getInstance();
    Date nextTime;
    Date lowWatermarkDate = extractFromTimestamp(Long.toString(lowWatermarkValue));
    Date highWatermarkDate = extractFromTimestamp(Long.toString(highWatermarkValue));
    final long lowWatermark = lowWatermarkDate.getTime();
    final long highWatermark = highWatermarkDate.getTime();

    int interval = getInterval(highWatermark - lowWatermark, partitionIntervalInHours, maxIntervals);
    LOG.info("Recalculated partition interval:" + interval + " hours");

    Date startTime = new Date(lowWatermark);
    Date endTime = new Date(highWatermark);
    LOG.debug("Start time:" + startTime + "; End time:" + endTime);
    long lwm;
    long hwm;
    while (startTime.getTime() < endTime.getTime()) {
      lwm = Long.parseLong(inputFormatParser.format(startTime));
      calendar.setTime(startTime);
      calendar.add(Calendar.HOUR, interval);
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
   * @param difference in range
   * @param hour interval (ex: 4 hours)
   * @param Maximum number of allowed partitions
   * @return calculated interval in hours
   */
  private static int getInterval(long diffInMilliSecs, long hourInterval, int maxIntervals) {
    int totalHours = DoubleMath.roundToInt((double) diffInMilliSecs / (60 * 60 * 1000), RoundingMode.CEILING);
    int totalIntervals = DoubleMath.roundToInt((double) totalHours / hourInterval, RoundingMode.CEILING);
    if (totalIntervals > maxIntervals) {
      hourInterval = DoubleMath.roundToInt((double) totalHours / maxIntervals, RoundingMode.CEILING);
    }
    return Ints.checkedCast(hourInterval);
  }

  /**
   * Convert timestamp to hour (yyyymmddHHmmss to yyyymmddHH)
   *
   * @param watermark value
   * @return value in hour format
   */
  synchronized private static Date extractFromTimestamp(String watermark) {
    final SimpleDateFormat inputFormat = new SimpleDateFormat(INPUTFORMAT);
    final SimpleDateFormat outputFormat = new SimpleDateFormat(OUTPUTFORMAT);
    Date outDate = null;
    try {
      Date date = inputFormat.parse(watermark);
      String dateStr = outputFormat.format(date);
      outDate = outputFormat.parse(dateStr);
    } catch (ParseException e) {
      LOG.error(e.getMessage(), e);
    }
    return outDate;
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
    Date date = Utils.toDate(baseWatermark, INPUTFORMAT, OUTPUTFORMAT);
    return Long.parseLong(parser.format(Utils.addHoursToDate(date, diff)));
  }
}
