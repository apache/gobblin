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

package org.apache.gobblin.multistage.util;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.joda.time.DateTime;


/**
 * A work unit that takes a range of days can be sub-divided into partitions
 * <p>
 * hourly partition will generate a work unit for each hour
 * <p>
 * daily partition will generate a work unit for each day
 * <p>
 * weekly partition will generate a work unit for each 7 days
 * <p>
 * monthly partition will generate a work unit for each 1 month
 * <p>
 * composite partitioning will generate a series of work units for each sub-type
 * <p>
 * The last partition can be partial if allow partial flag is true.
 *
 * TODO: when other types of watermarks are supported, the sub-partition might need
 * support hash partition or even space partitions
 *
 * @author chrli
 *
 */
@Slf4j
public enum WorkUnitPartitionTypes {
  NONE("none", 0) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return end;
    }
  },
  HOURLY("hourly", 1) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return start.plusHours(interval);
    }
  },
  DAILY("daily", 1) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return start.plusDays(interval);
    }
  },
  WEEKLY("weekly", 7, true) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return start.plusDays(interval);
    }
  },
  MONTHLY("monthly", 1, true) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return start.plusMonths(interval);
    }
  },
  YEARLY("yearly", 1, true) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      return start.plusYears(interval);
    }
  },
  COMPOSITE("composite", 0, true) {
    @Override
    protected DateTime getNext(DateTime start, DateTime end) {
      throw new RuntimeException("Composite should never call itself!");
    }

    /**
     * Refer to the specifications and constraints for "ms.work.unit.partition" in MultistageProperties.
     * The ranges should be continuous with no gaps or overlaps.
     *
     */
    @Override
    protected Pair<DateTime, DateTime> getNext(DateTime start, DateTime end, boolean allowPartial) {
      // Start from the lowest range that has the smallest start date time
      // Get the first range that matches - it would also satisfy the partial partitioning config.
      Pair<DateTime, DateTime> nextDateTime;
      for (SubRange subRange: subRanges) {
        if (subRange.left.getMillis() <= end.getMillis()
            && subRange.right.getMillis() > start.getMillis()) {
          WorkUnitPartitionTypes subRangeWorkUnitPartitionType = subRange.partitionType;
          /*
           Handling a case where the previous subrange's last partial partition was ignored because
           partial partitioning is false.
           */
          DateTime startToUse = subRange.left.getMillis() > start.getMillis() ? subRange.left : start;
          nextDateTime =  subRangeWorkUnitPartitionType.getNext(startToUse, subRange.right, allowPartial);
          if (nextDateTime != null) {
            return nextDateTime;
          }
        }
      }
      return null;
    }
  };

  final private static Gson GSON = new Gson();

  final private String partitionType;
  final protected Integer interval;
  final private Boolean isMultiDayPartitioned;
  final protected SortedSet<SubRange> subRanges;

  WorkUnitPartitionTypes(String partitionType, Integer interval) {
    this(partitionType, interval, false);
  }

  WorkUnitPartitionTypes(String partitionType, Integer interval, Boolean isMultiDayPartitioned) {
    this(partitionType, interval, isMultiDayPartitioned, new TreeSet<>());
  }

  WorkUnitPartitionTypes(String partitionType, Integer interval, Boolean isMultiDayPartitioned,
      SortedSet<SubRange> subRanges) {
    this.partitionType = partitionType;
    this.interval = interval;
    this.isMultiDayPartitioned = isMultiDayPartitioned;
    this.subRanges = subRanges;
  }

  /**
   *
   * Static method to parse a string and return the partition type
   * @param partitionType specified partition types or a JsonObject
   * @return specified partition types or COMPOSITE
   *
   */
  public static WorkUnitPartitionTypes fromString(String partitionType) {
    for (WorkUnitPartitionTypes workUnitPartitionType : WorkUnitPartitionTypes.values()) {
      if (workUnitPartitionType.partitionType.equalsIgnoreCase(partitionType)) {
        return workUnitPartitionType;
      }
    }
    try {
      JsonObject jsonObject = GSON.fromJson(partitionType, JsonObject.class);
      if (jsonObject.entrySet().size() > 0) {
        return WorkUnitPartitionTypes.COMPOSITE;
      }
    } catch (Exception e) {
      log.error("Error parsing the partition type string, please check job property: "
          + MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.toString(), e);
    }
    return null;
  }

  public static boolean isMultiDayPartitioned(WorkUnitPartitionTypes workUnitPartitionType) {
    return (workUnitPartitionType != null && workUnitPartitionType.isMultiDayPartitioned);
  }

  @Override
  public String toString() {
    return partitionType;
  }

  protected abstract DateTime getNext(DateTime start, DateTime end);

  protected Pair<DateTime, DateTime> getNext(DateTime start, DateTime end, boolean allowPartial) {
    DateTime next = getNext(start, end);
    if (next.getMillis() <= end.getMillis()) {
      return Pair.of(start, next);
    } else if (allowPartial) {
      return Pair.of(start, end);
    } else {
      return null;
    }
  }

  public List<ImmutablePair<Long, Long>> getRanges(Pair<DateTime, DateTime> range) {
    return getRanges(range, false);
  }

  public List<ImmutablePair<Long, Long>> getRanges(Pair<DateTime, DateTime> range, boolean allowPartial) {
    return getRanges(range.getLeft(), range.getRight(), allowPartial);
  }

  public List<ImmutablePair<Long, Long>> getRanges(DateTime start, DateTime end) {
    return getRanges(start, end, false);
  }

  /**
   * Convert DateTime range to a list of milli-second ranges.
   *
   * @param start start date time with time zone enclosed
   * @param end end date time with time zone enclosed
   * @param allowPartial whether the last partition can be partial
   * @return a list of milli-second ranges
   */
  public List<ImmutablePair<Long, Long>> getRanges(DateTime start, DateTime end, boolean allowPartial) {
    List<ImmutablePair<Long, Long>> list = Lists.newArrayList();
    DateTime tempStart = start;
    while (tempStart.getMillis() < end.getMillis()) {
      Pair<DateTime, DateTime> nextDateTimeRange = getNext(tempStart, end, allowPartial);
      if (nextDateTimeRange != null) {
        list.add(new ImmutablePair<>(nextDateTimeRange.getLeft().getMillis(), nextDateTimeRange.getRight().getMillis()));
        /*
           Composite partitioning could choose next subrange's start date
           => because previous subrange's last partial partition was ignored
           => because partial partitioning is false.
         */
        tempStart = nextDateTimeRange.getRight();
      } else {
        tempStart = end;
      }
    }
    return list;
  }

  /**
   * Add a sub range and its partition method
   * @param start the start date time of the range
   * @param end the end date time of the range
   * @param partitionTypes the partition type
   * @return the object itself so that operation can be chained if needed
   */
  public WorkUnitPartitionTypes addSubRange(DateTime start, DateTime end, WorkUnitPartitionTypes partitionTypes) {
    this.subRanges.add(new SubRange(start, end, partitionTypes));
    return this;
  }

  /**
   * Clear the sub ranges
   * @return the object itself
   */
  public WorkUnitPartitionTypes resetSubRange() {
    this.subRanges.clear();
    return this;
  }

  private class SubRange implements Comparable<SubRange> {
    DateTime left;
    DateTime right;
    WorkUnitPartitionTypes partitionType;

    public SubRange(DateTime left, DateTime right, WorkUnitPartitionTypes partitionType) {
      this.left = left;
      this.right = right;
      this.partitionType = partitionType;
    }

    @Override
    public int compareTo(SubRange o) {
      return this.left.compareTo(o.left);
    }
  }
}