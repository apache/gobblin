/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.watermark;

import gobblin.source.extractor.extract.QueryBasedExtractor;
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


public class HourWatermark implements Watermark {
  private static final Logger LOG = LoggerFactory.getLogger(HourWatermark.class);
  // default water mark format(input format) example: 20140301050505
  private static final String INPUTFORMAT = "yyyyMMddHHmmss";
  // output format of hour water mark example: 2014030105
  private static final String OUTPUTFORMAT = "yyyyMMddHH";
  private static final int deltaForNextWatermark = 60 * 60;
  private static final SimpleDateFormat INPUTFORMATPARSER = new SimpleDateFormat(INPUTFORMAT);

  private String watermarkColumn;
  private String watermarkFormat;

  public HourWatermark(String watermarkColumn, String watermarkFormat) {
    this.watermarkColumn = watermarkColumn;
    this.watermarkFormat = watermarkFormat;
  }

  @Override
  public String getWatermarkCondition(QueryBasedExtractor extractor, long watermarkValue, String operator) {
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
    HashMap<Long, Long> intervalMap = new HashMap<Long, Long>();

    if (lowWatermarkValue > highWatermarkValue) {
      LOG.warn("The low water mark is greater than the high water mark, empty intervals are returned");
      return intervalMap;
    }

    final Calendar calendar = Calendar.getInstance();
    Date nextTime;
    Date lowWatermarkDate = this.extractFromTimestamp(Long.toString(lowWatermarkValue));
    Date highWatermarkDate = this.extractFromTimestamp(Long.toString(highWatermarkValue));
    final long lowWatermark = lowWatermarkDate.getTime();
    final long highWatermark = highWatermarkDate.getTime();

    int interval = this.getInterval(highWatermark - lowWatermark, partitionIntervalInHours, maxIntervals);
    LOG.info("Recalculated partition interval:" + interval + " hours");

    Date startTime = new Date(lowWatermark);
    Date endTime = new Date(highWatermark);
    LOG.debug("Start time:" + startTime + "; End time:" + endTime);
    long lwm;
    long hwm;
    while (startTime.getTime() <= endTime.getTime()) {
      lwm = Long.parseLong(INPUTFORMATPARSER .format(startTime));
      calendar.setTime(startTime);
      calendar.add(Calendar.HOUR, interval - 1);
      nextTime = calendar.getTime();
      hwm = Long.parseLong(INPUTFORMATPARSER.format(nextTime.getTime() <= endTime.getTime() ? nextTime : endTime));
      intervalMap.put(lwm, hwm);
      LOG.debug("Partition - low:" + lwm + "; high:" + hwm);
      calendar.add(Calendar.SECOND, deltaForNextWatermark);
      startTime = calendar.getTime();
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
  private int getInterval(long diffInMilliSecs, long hourInterval, int maxIntervals) {
    int totalHours = DoubleMath.roundToInt((double) diffInMilliSecs / (60 * 60 * 1000), RoundingMode.CEILING);
    int totalIntervals = DoubleMath.roundToInt((double) totalHours / hourInterval, RoundingMode.CEILING);
    if (totalIntervals > maxIntervals) {
      hourInterval = DoubleMath.roundToInt((double) totalHours / maxIntervals, RoundingMode.CEILING);
    }
    return Ints.checkedCast(hourInterval) + 1;
  }

  /**
   * Convert timestamp to hour (yyyymmddHHmmss to yyyymmddHH)
   *
   * @param watermark value
   * @return value in hour format
   */
  synchronized private Date extractFromTimestamp(String watermark) {
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
}
