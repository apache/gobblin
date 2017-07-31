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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.ExtractType;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.extractor.watermark.DateWatermark;
import org.apache.gobblin.source.extractor.watermark.HourWatermark;
import org.apache.gobblin.source.extractor.watermark.SimpleWatermark;
import org.apache.gobblin.source.extractor.watermark.TimestampWatermark;
import org.apache.gobblin.source.extractor.watermark.WatermarkPredicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;


/**
 * An implementation of default partitioner for all types of sources
 */
public class Partitioner {
  private static final Logger LOG = LoggerFactory.getLogger(Partitioner.class);

  public static final String WATERMARKTIMEFORMAT = "yyyyMMddHHmmss";
  public static final String HAS_USER_SPECIFIED_PARTITIONS = "partitioner.hasUserSpecifiedPartitions";
  public static final String USER_SPECIFIED_PARTITIONS = "partitioner.userSpecifiedPartitions";

  public static final Comparator<Partition> ascendingComparator = new Comparator<Partition>() {
    @Override
    public int compare(Partition p1, Partition p2) {
      if (p1 == null && p2 == null) {
        return 0;
      }
      if (p1 == null) {
        return -1;
      }
      if (p2 == null) {
        return 1;
      }
      return Long.compare(p1.getLowWatermark(), p2.getLowWatermark());
    }
  };

  private SourceState state;

  /**
   * Indicate if the user specifies a high watermark for the current run
   */
  @VisibleForTesting
  protected boolean hasUserSpecifiedHighWatermark;

  public Partitioner(SourceState state) {
    super();
    this.state = state;
    hasUserSpecifiedHighWatermark = false;
  }

  /**
   * Get the global partition of the whole data set, which has the global low and high watermarks
   *
   * @param previousWatermark previous watermark for computing the low watermark of current run
   * @return a Partition instance
   */
  public Partition getGlobalPartition(long previousWatermark) {
    ExtractType extractType =
        ExtractType.valueOf(state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE).toUpperCase());
    WatermarkType watermarkType = WatermarkType.valueOf(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());

    WatermarkPredicate watermark = new WatermarkPredicate(null, watermarkType);
    int deltaForNextWatermark = watermark.getDeltaNumForNextWatermark();

    long lowWatermark = getLowWatermark(extractType, watermarkType, previousWatermark, deltaForNextWatermark);
    long highWatermark = getHighWatermark(extractType, watermarkType);
    return new Partition(lowWatermark, highWatermark, true, hasUserSpecifiedHighWatermark);
  }

  /**
   * Get partitions with low and high water marks
   *
   * @param previousWatermark previous water mark from metadata
   * @return map of partition intervals
   */
  @Deprecated
  public HashMap<Long, Long> getPartitions(long previousWatermark) {
    HashMap<Long, Long> defaultPartition = Maps.newHashMap();
    if (!isWatermarkExists()) {
      defaultPartition.put(ConfigurationKeys.DEFAULT_WATERMARK_VALUE, ConfigurationKeys.DEFAULT_WATERMARK_VALUE);
      LOG.info("Watermark column or type not found - Default partition with low watermark and high watermark as "
          + ConfigurationKeys.DEFAULT_WATERMARK_VALUE);
      return defaultPartition;
    }

    ExtractType extractType =
        ExtractType.valueOf(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE).toUpperCase());
    WatermarkType watermarkType = WatermarkType.valueOf(
        this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());
    int interval =
        getUpdatedInterval(this.state.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_PARTITION_INTERVAL, 0),
            extractType, watermarkType);
    int sourceMaxAllowedPartitions = this.state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, 0);
    int maxPartitions = (sourceMaxAllowedPartitions != 0 ? sourceMaxAllowedPartitions
        : ConfigurationKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS);

    WatermarkPredicate watermark = new WatermarkPredicate(null, watermarkType);
    int deltaForNextWatermark = watermark.getDeltaNumForNextWatermark();

    LOG.info("is watermark override: " + this.isWatermarkOverride());
    LOG.info("is full extract: " + this.isFullDump());
    long lowWatermark = this.getLowWatermark(extractType, watermarkType, previousWatermark, deltaForNextWatermark);
    long highWatermark = this.getHighWatermark(extractType, watermarkType);

    if (lowWatermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE
        || highWatermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
      LOG.info(
          "Low watermark or high water mark is not found. Hence cannot generate partitions - Default partition with low watermark:  "
              + lowWatermark + " and high watermark: " + highWatermark);
      defaultPartition.put(lowWatermark, highWatermark);
      return defaultPartition;
    }
    LOG.info("Generate partitions with low watermark: " + lowWatermark + "; high watermark: " + highWatermark
        + "; partition interval in hours: " + interval + "; Maximum number of allowed partitions: " + maxPartitions);
    return watermark.getPartitions(lowWatermark, highWatermark, interval, maxPartitions);
  }

  /**
   * Get an unordered list of partition with lowWatermark, highWatermark, and hasUserSpecifiedHighWatermark.
   *
   * @param previousWatermark previous water mark from metadata
   * @return an unordered list of partition
   */
  public List<Partition> getPartitionList(long previousWatermark) {
    if (state.getPropAsBoolean(HAS_USER_SPECIFIED_PARTITIONS)) {
      return createUserSpecifiedPartitions();
    }

    List<Partition> partitions = new ArrayList<>();

    /*
     * Use the deprecated getPartitions(long) as a helper function, avoid duplicating logic. When it can be removed, its
     * logic will be put here.
     */
    HashMap<Long, Long> partitionMap = getPartitions(previousWatermark);

    /*
     * Can't use highWatermark directly, as the partitionMap may have different precision. For example, highWatermark
     * may be specified to seconds, but partitionMap could be specified to hour or date.
     */
    Long highestWatermark = Collections.max(partitionMap.values());

    for (Map.Entry<Long, Long> entry : partitionMap.entrySet()) {
      Long partitionHighWatermark = entry.getValue();
      // Apply hasUserSpecifiedHighWatermark to the last partition, which has highestWatermark
      if (partitionHighWatermark.equals(highestWatermark)) {
        partitions.add(new Partition(entry.getKey(), partitionHighWatermark, true, hasUserSpecifiedHighWatermark));
      } else {
        // The partitionHighWatermark was computed on the fly not what user specifies
        partitions.add(new Partition(entry.getKey(), partitionHighWatermark, false));
      }
    }
    return partitions;
  }

  /**
   * Generate the partitions based on the lists specified by the user in job config
   */
  private List<Partition> createUserSpecifiedPartitions() {
    List<Partition> partitions = new ArrayList<>();

    List<String> watermarkPoints = state.getPropAsList(USER_SPECIFIED_PARTITIONS);
    if (watermarkPoints == null || watermarkPoints.size() == 0 ) {
      LOG.info("There should be some partition points");
      long defaultWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
      partitions.add(new Partition(defaultWatermark, defaultWatermark, true, true));
      return partitions;
    }

    WatermarkType watermarkType = WatermarkType.valueOf(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());

    long lowWatermark = adjustWatermark(watermarkPoints.get(0), watermarkType);
    long highWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    // Only one partition point specified
    if (watermarkPoints.size() == 1) {
      if (watermarkType != WatermarkType.SIMPLE) {
        String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE);
        String currentTime = Utils.dateTimeToString(getCurrentTime(timeZone), WATERMARKTIMEFORMAT, timeZone);
        highWatermark = adjustWatermark(currentTime, watermarkType);
      }
      partitions.add(new Partition(lowWatermark, highWatermark, true, false));
      return partitions;
    }

    int i;
    for (i = 1; i < watermarkPoints.size() - 1; i++) {
      highWatermark = adjustWatermark(watermarkPoints.get(i), watermarkType);
      partitions.add(new Partition(lowWatermark, highWatermark, true));
      lowWatermark = highWatermark;
    }

    // Last partition
    highWatermark = adjustWatermark(watermarkPoints.get(i), watermarkType);
    ExtractType extractType =
        ExtractType.valueOf(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE).toUpperCase());
    if (isFullDump() || isSnapshot(extractType)) {
      // The upper bounds can be removed for last work unit
      partitions.add(new Partition(lowWatermark, highWatermark, true, false));
    } else {
      // The upper bounds can not be removed for last work unit
      partitions.add(new Partition(lowWatermark, highWatermark, true, true));
    }

    return partitions;
  }

  /**
   * Adjust a watermark based on watermark type
   *
   * @param baseWatermark the original watermark
   * @param watermarkType Watermark Type
   * @return the adjusted watermark value
   */
  private static long adjustWatermark(String baseWatermark, WatermarkType watermarkType) {
    long result = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    switch (watermarkType) {
      case SIMPLE:
        result = SimpleWatermark.adjustWatermark(baseWatermark, 0);
        break;
      case DATE:
        result = DateWatermark.adjustWatermark(baseWatermark, 0);
        break;
      case HOUR:
        result = HourWatermark.adjustWatermark(baseWatermark, 0);
        break;
      case TIMESTAMP:
        result = TimestampWatermark.adjustWatermark(baseWatermark, 0);
        break;
    }
    return result;
  }

  /**
   * Calculate interval in hours with the given interval
   *
   * @param inputInterval input interval
   * @param extractType Extract type
   * @param watermarkType Watermark type
   * @return interval in range
   */
  private static int getUpdatedInterval(int inputInterval, ExtractType extractType, WatermarkType watermarkType) {
    LOG.debug("Getting updated interval");
    if ((extractType == ExtractType.SNAPSHOT && watermarkType == WatermarkType.DATE)) {
      return inputInterval * 24;
    } else if (extractType == ExtractType.APPEND_DAILY) {
      return (inputInterval < 1 ? 1 : inputInterval) * 24;
    } else {
      return inputInterval;
    }
  }

  /**
   * Get low water mark
   *
   * @param extractType Extract type
   * @param watermarkType Watermark type
   * @param previousWatermark Previous water mark
   * @param deltaForNextWatermark delta number for next water mark
   * @return low water mark
   */
  @VisibleForTesting
  protected long getLowWatermark(ExtractType extractType, WatermarkType watermarkType, long previousWatermark,
      int deltaForNextWatermark) {
    long lowWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    if (this.isFullDump() || this.isWatermarkOverride()) {
      String timeZone =
          this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE, ConfigurationKeys.DEFAULT_SOURCE_TIMEZONE);
      /*
       * SOURCE_QUERYBASED_START_VALUE could be:
       *  - a simple string, e.g. "12345"
       *  - a timestamp string, e.g. "20140101000000"
       *  - a string with a time directive, e.g. "CURRENTDAY-X", "CURRENTHOUR-X", (X is a number)
       */
      lowWatermark =
          Utils.getLongWithCurrentDate(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE), timeZone);
      LOG.info("Overriding low water mark with the given start value: " + lowWatermark);
    } else {
      if (isSnapshot(extractType)) {
        lowWatermark = this.getSnapshotLowWatermark(watermarkType, previousWatermark, deltaForNextWatermark);
      } else {
        lowWatermark = this.getAppendLowWatermark(watermarkType, previousWatermark, deltaForNextWatermark);
      }
    }
    return (lowWatermark == 0 ? ConfigurationKeys.DEFAULT_WATERMARK_VALUE : lowWatermark);
  }

  /**
   * Get low water mark
   *
   * @param watermarkType Watermark type
   * @param previousWatermark Previous water mark
   * @param deltaForNextWatermark delta number for next water mark
   * @return snapshot low water mark
   */
  private long getSnapshotLowWatermark(WatermarkType watermarkType, long previousWatermark, int deltaForNextWatermark) {
    LOG.debug("Getting snapshot low water mark");
    String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE, ConfigurationKeys.DEFAULT_SOURCE_TIMEZONE);
    if (isPreviousWatermarkExists(previousWatermark)) {
      if (isSimpleWatermark(watermarkType)) {
        return previousWatermark + deltaForNextWatermark - this.state
            .getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, 0);
      }
      DateTime wm = Utils.toDateTime(previousWatermark, WATERMARKTIMEFORMAT, timeZone).plusSeconds(
          (deltaForNextWatermark - this.state
              .getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, 0)));

      return Long.parseLong(Utils.dateTimeToString(wm, WATERMARKTIMEFORMAT, timeZone));
    }

    // If previous watermark is not found, override with the start value
    // (irrespective of source.is.watermark.override flag)
    long startValue =
        Utils.getLongWithCurrentDate(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE), timeZone);
    LOG.info("Overriding low water mark with the given start value: " + startValue);
    return startValue;
  }

  /**
   * Get low water mark
   *
   * @param watermarkType Watermark type
   * @param previousWatermark Previous water mark
   * @param deltaForNextWatermark delta number for next water mark
   * @return append low water mark
   */
  private long getAppendLowWatermark(WatermarkType watermarkType, long previousWatermark, int deltaForNextWatermark) {
    LOG.debug("Getting append low water mark");
    String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE);
    if (isPreviousWatermarkExists(previousWatermark)) {
      if (isSimpleWatermark(watermarkType)) {
        return previousWatermark + deltaForNextWatermark;
      }
      DateTime wm =
          Utils.toDateTime(previousWatermark, WATERMARKTIMEFORMAT, timeZone).plusSeconds(deltaForNextWatermark);
      return Long.parseLong(Utils.dateTimeToString(wm, WATERMARKTIMEFORMAT, timeZone));
    }
    LOG.info("Overriding low water mark with start value: " + ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE);
    return Utils.getLongWithCurrentDate(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_START_VALUE), timeZone);
  }

  /**
   * Get high water mark
   *
   * @param extractType Extract type
   * @param watermarkType Watermark type
   * @return high water mark
   */
  @VisibleForTesting
  protected long getHighWatermark(ExtractType extractType, WatermarkType watermarkType) {
    LOG.debug("Getting high watermark");
    String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE);
    long highWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    if (this.isWatermarkOverride()) {
      highWatermark = this.state.getPropAsLong(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, 0);
      if (highWatermark == 0) {
        highWatermark = Long.parseLong(Utils.dateTimeToString(getCurrentTime(timeZone), WATERMARKTIMEFORMAT, timeZone));
      } else {
        // User specifies SOURCE_QUERYBASED_END_VALUE
        hasUserSpecifiedHighWatermark = true;
      }
      LOG.info("Overriding high water mark with the given end value:" + highWatermark);
    } else {
      if (isSnapshot(extractType)) {
        highWatermark = this.getSnapshotHighWatermark(watermarkType);
      } else {
        highWatermark = this.getAppendHighWatermark(extractType);
      }
    }
    return (highWatermark == 0 ? ConfigurationKeys.DEFAULT_WATERMARK_VALUE : highWatermark);
  }

  /**
   * Get snapshot high water mark
   *
   * @param watermarkType Watermark type
   * @return snapshot high water mark
   */
  private long getSnapshotHighWatermark(WatermarkType watermarkType) {
    LOG.debug("Getting snapshot high water mark");
    if (isSimpleWatermark(watermarkType)) {
      return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    }
    String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE);
    return Long.parseLong(Utils.dateTimeToString(getCurrentTime(timeZone), WATERMARKTIMEFORMAT, timeZone));
  }

  /**
   * Get append high water mark
   *
   * @param extractType Extract type
   * @return append high water mark
   */
  private long getAppendHighWatermark(ExtractType extractType) {
    LOG.debug("Getting append high water mark");
    if (this.isFullDump()) {
      LOG.info("Overriding high water mark with end value:" + ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE);
      long highWatermark = this.state.getPropAsLong(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, 0);
      if (highWatermark != 0) {
        // User specifies SOURCE_QUERYBASED_END_VALUE
        hasUserSpecifiedHighWatermark = true;
      }
      return highWatermark;
    }
    return this.getAppendWatermarkCutoff(extractType);
  }

  /**
   * Get cutoff for high water mark
   *
   * @param extractType Extract type
   * @return cutoff
   */
  private long getAppendWatermarkCutoff(ExtractType extractType) {
    LOG.debug("Getting append water mark cutoff");
    long highWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    String timeZone = this.state.getProp(ConfigurationKeys.SOURCE_TIMEZONE);
    AppendMaxLimitType limitType = getAppendLimitType(extractType,
        this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT));
    if (limitType == null) {
      LOG.debug("Limit type is not found");
      return highWatermark;
    }
    int limitDelta =
        getAppendLimitDelta(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT));
    // if it is CURRENTDATE or CURRENTHOUR then high water mark is current time
    if (limitDelta == 0) {
      highWatermark = Long.parseLong(Utils.dateTimeToString(getCurrentTime(timeZone), WATERMARKTIMEFORMAT, timeZone));
    }
    // if CURRENTDATE or CURRENTHOUR has offset then high water mark is end of day of the given offset
    else {
      int seconds = 3599; // x:59:59
      String format = null;

      switch (limitType) {
        case CURRENTDATE:
          format = "yyyyMMdd";
          limitDelta = limitDelta * 24 * 60 * 60;
          seconds = 86399; // 23:59:59
          break;
        case CURRENTHOUR:
          format = "yyyyMMddHH";
          limitDelta = limitDelta * 60 * 60;
          seconds = 3599; // x:59:59
          break;
        case CURRENTMINUTE:
          format = "yyyyMMddHHmm";
          limitDelta = limitDelta * 60;
          seconds = 59;
          break;
        case CURRENTSECOND:
          format = "yyyyMMddHHmmss";
          seconds = 0;
          break;
        default:
          break;
      }

      DateTime deltaTime = getCurrentTime(timeZone).minusSeconds(limitDelta);
      DateTime previousTime =
          Utils.toDateTime(Utils.dateTimeToString(deltaTime, format, timeZone), format, timeZone).plusSeconds(seconds);
      highWatermark = Long.parseLong(Utils.dateTimeToString(previousTime, WATERMARKTIMEFORMAT, timeZone));

      // User specifies SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT
      hasUserSpecifiedHighWatermark = true;
    }
    return highWatermark;
  }

  /**
   * Get append max limit type from the input
   *
   * @param extractType Extract type
   * @param maxLimit
   * @return Max limit type
   */
  private static AppendMaxLimitType getAppendLimitType(ExtractType extractType, String maxLimit) {
    LOG.debug("Getting append limit type");
    AppendMaxLimitType limitType;
    switch (extractType) {
      case APPEND_DAILY:
        limitType = AppendMaxLimitType.CURRENTDATE;
        break;
      case APPEND_HOURLY:
        limitType = AppendMaxLimitType.CURRENTHOUR;
        break;
      default:
        limitType = null;
        break;
    }

    if (!Strings.isNullOrEmpty(maxLimit)) {
      LOG.debug("Getting append limit type from the config");
      String[] limitParams = maxLimit.split("-");
      if (limitParams.length >= 1) {
        limitType = AppendMaxLimitType.valueOf(limitParams[0]);
      }
    }
    return limitType;
  }

  /**
   * Get append max limit delta num
   *
   * @param maxLimit
   * @return Max limit delta number
   */
  private static int getAppendLimitDelta(String maxLimit) {
    LOG.debug("Getting append limit delta");
    int limitDelta = 0;
    if (!Strings.isNullOrEmpty(maxLimit)) {
      String[] limitParams = maxLimit.split("-");
      if (limitParams.length >= 2) {
        limitDelta = Integer.parseInt(limitParams[1]);
      }
    }
    return limitDelta;
  }

  /**
   * true if previous water mark equals default water mark
   *
   * @param previousWatermark previous water mark
   * @return true if previous water mark exists
   */
  private static boolean isPreviousWatermarkExists(long previousWatermark) {
    if (!(previousWatermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE)) {
      return true;
    }
    return false;
  }

  /**
   * true if water mark columns and water mark type provided
   *
   * @return true if water mark exists
   */
  private boolean isWatermarkExists() {
    if (!Strings.isNullOrEmpty(this.state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY)) && !Strings
        .isNullOrEmpty(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE))) {
      return true;
    }
    return false;
  }

  private static boolean isSnapshot(ExtractType extractType) {
    if (extractType == ExtractType.SNAPSHOT) {
      return true;
    }
    return false;
  }

  private static boolean isSimpleWatermark(WatermarkType watermarkType) {
    if (watermarkType == WatermarkType.SIMPLE) {
      return true;
    }
    return false;
  }

  /**
   * @return full dump or not
   */
  public boolean isFullDump() {
    return Boolean.valueOf(this.state.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY));
  }

  /**
   * @return full dump or not
   */
  public boolean isWatermarkOverride() {
    return Boolean.valueOf(this.state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE));
  }

  /**
   * This thin function is introduced to facilitate testing, a way to mock current time
   *
   * @return current time in the given timeZone
   */
  @VisibleForTesting
  public DateTime getCurrentTime(String timeZone) {
    return Utils.getCurrentTime(timeZone);
  }
}
