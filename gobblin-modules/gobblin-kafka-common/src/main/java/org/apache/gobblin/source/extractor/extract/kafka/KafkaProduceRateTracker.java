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

import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Maps;

import de.jollyday.HolidayManager;
import de.jollyday.parameter.UrlManagerParameter;
import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.FlushingExtractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.writer.WatermarkTracker;

/**
 * A helper class that tracks the produce rate for each TopicPartition currently consumed by the {@link KafkaStreamingExtractor}.
 * The produce rates are stored in the {@link org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor.KafkaWatermark}
 * and checkpointed to the {@link org.apache.gobblin.writer.WatermarkStorage} along with the watermarks.
 *
 * The produce rates are maintained based on hour-of-day and day-of-week, and are computed in bytes/sec. The new produce
 * rates estimates are obtained as Exponentially Weighted Moving Average (EWMA) as:
 * new_produce_rate = a * avg_rate_in_current_window + (1 - a) * historic_produce_rate, where:
 * "a" is the exponential decay factor. Small values of "a" result in estimates updated more slowly, while large values of
 * "a" will result in giving more weight to recent values.
 */
public class KafkaProduceRateTracker {
  private static final String KAFKA_PRODUCE_RATE_EXPONENTIAL_DECAY_FACTOR_KEY =
      "gobblin.kafka.produceRateTracker.exponentialDecayFactor";
  private static final Double DEFAULT_KAFKA_PRODUCE_RATE_EXPONENTIAL_DECAY_FACTOR = 0.375;
  static final String KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY = "gobblin.kafka.produceRateTracker.disableStatsOnHolidays";
  private static final Boolean DEFAULT_KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS = false;
  private static final String KAFKA_PRODUCE_RATE_HOLIDAY_LOCALE_KEY = "gobblin.kafka.produceRateTracker.holidayLocale";
  private static final String DEFAULT_KAFKA_PRODUCE_RATE_HOLIDAY_LOCALE = "ca";
  private static final String HOLIDAY_FILE = "Holidays.xml";
  private static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.getDefault();
  static final int SLIDING_WINDOW_SIZE = 3;

  static final String KAFKA_PARTITION_PRODUCE_RATE_KEY = "produceRate";

  /**
   * The element-insertion order has to be maintained since:
   * 1. API provided by {@link KafkaExtractorStatsTracker} accepts index of partition, instead of partition
   * object like {@link #partitionsToProdRate}.
   * 2. When traversing {@link #partitionsToProdRate} to update new produce-rate value, we need find the previous value
   * of watermark to help calculate produce-rate, which is also indexed by partition-index.
   * 3. To make sure the entry mapping between entry in the {@link #partitionsToProdRate} and entry in {@link KafkaExtractorStatsTracker}
   * and watermark, the order of entries in this map which is defined by input list of {@link KafkaPartition} needs
   * to be preserved, as this input list also serves source of constructing entry-order in {@link KafkaExtractorStatsTracker}.
   */
  @Getter
  private final LinkedHashMap<KafkaPartition, Double> partitionsToProdRate;
  private final WatermarkTracker watermarkTracker;
  private final KafkaExtractorStatsTracker statsTracker;
  private final Double exponentialDecayFactor;
  private final Boolean disableStatsOnHolidays;
  private final HolidayManager holidayManager;
  private final long flushIntervalSecs;
  private final String holidayLocale;
  private Long lastReportTimeMillis;
  private final Map<LocalDate, Boolean> holidayMap = Maps.newHashMap();

  private final EvictingQueue<Long> ingestionLatencies = EvictingQueue.create(SLIDING_WINDOW_SIZE);
  private final EvictingQueue<Double> consumptionRateMBps = EvictingQueue.create(SLIDING_WINDOW_SIZE);

  public KafkaProduceRateTracker(WorkUnitState state, List<KafkaPartition> partitions, WatermarkTracker watermarkTracker,
      KafkaExtractorStatsTracker statsTracker) {
    this(state, partitions, watermarkTracker, statsTracker, System.currentTimeMillis());
  }

  @VisibleForTesting
  KafkaProduceRateTracker(WorkUnitState state, List<KafkaPartition> partitions, WatermarkTracker watermarkTracker,
      KafkaExtractorStatsTracker statsTracker, Long lastReportTimeMillis) {
    this.partitionsToProdRate = (LinkedHashMap<KafkaPartition, Double>) partitions.stream()
        .collect(Collectors.toMap(Function.identity(), x -> new Double(-1), (e1, e2) -> e1, LinkedHashMap::new));
    this.watermarkTracker = watermarkTracker;
    this.statsTracker = statsTracker;
    this.lastReportTimeMillis = lastReportTimeMillis;
    this.exponentialDecayFactor = state.getPropAsDouble(KAFKA_PRODUCE_RATE_EXPONENTIAL_DECAY_FACTOR_KEY, DEFAULT_KAFKA_PRODUCE_RATE_EXPONENTIAL_DECAY_FACTOR);

    URL calendarFileUrl = getClass().getClassLoader().getResource(HOLIDAY_FILE);
    this.holidayManager =
        calendarFileUrl != null ? HolidayManager.getInstance(new UrlManagerParameter(calendarFileUrl, new Properties()))
            : HolidayManager.getInstance();
    this.disableStatsOnHolidays = state.getPropAsBoolean(KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS_KEY,
        DEFAULT_KAFKA_PRODUCE_RATE_DISABLE_STATS_ON_HOLIDAYS);
    this.holidayLocale = state.getProp(KAFKA_PRODUCE_RATE_HOLIDAY_LOCALE_KEY, DEFAULT_KAFKA_PRODUCE_RATE_HOLIDAY_LOCALE);
    this.flushIntervalSecs = state.getPropAsLong(FlushingExtractor.FLUSH_INTERVAL_SECONDS_KEY, FlushingExtractor.DEFAULT_FLUSH_INTERVAL_SECONDS);
  }

  public static int getHourOfDay(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    //HOUR_OF_DAY ranges from 0-23.
    return calendar.get(Calendar.HOUR_OF_DAY);
  }

  public static int getDayOfWeek(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    //DAY_OF_WEEK ranges from 1-7.
    return calendar.get(Calendar.DAY_OF_WEEK) - 1;
  }

  @Getter
  @AllArgsConstructor
  public static class TopicPartitionStats {
    private double[][] avgProduceRates;
    private double avgConsumeRate;
    private long avgRecordSize;
    // Cache the recent caught produce-rate of a partition.
    private double currentProduceRate;
  }

  private TopicPartitionStats getNewTopicPartitionStats(Long previousMaxOffset, Long maxOffset, KafkaStreamingExtractor.KafkaWatermark lastCommittedWatermark,
      long currentTimeMillis, Long avgRecordSize) {
    if (previousMaxOffset == 0) {
      TopicPartitionStats stats = new TopicPartitionStats(lastCommittedWatermark.getAvgProduceRates(), lastCommittedWatermark.getAvgConsumeRate(),
          lastCommittedWatermark.getAvgRecordSize(), 0);
      return stats;
    }
    long numRecordsProduced = maxOffset - previousMaxOffset;
    long newAvgRecordSize;
    if (numRecordsProduced > 0) {
      if (lastCommittedWatermark.getAvgRecordSize() > 0) {
        newAvgRecordSize = updateMovingAverage(avgRecordSize, lastCommittedWatermark.getAvgRecordSize()).longValue();
      } else {
        //No previously recorded average record size.
        newAvgRecordSize = avgRecordSize;
      }
    } else {
      //No records see in the current window. No need to update the average record size.
      newAvgRecordSize = lastCommittedWatermark.getAvgRecordSize();
    }
    Date date = new Date(currentTimeMillis);
    int hourOfDay = getHourOfDay(date);
    int dayOfWeek = getDayOfWeek(date);
    double currentProduceRate =
        (numRecordsProduced * avgRecordSize) * 1000 / (double) (currentTimeMillis - lastReportTimeMillis + 1);
    double[][] historicProduceRates = lastCommittedWatermark.getAvgProduceRates();
    if (!isHoliday(new LocalDate(currentTimeMillis, DEFAULT_TIME_ZONE))) {
      if (historicProduceRates != null) {
        if (historicProduceRates[dayOfWeek][hourOfDay] >= 0) {
          historicProduceRates[dayOfWeek][hourOfDay] =
              updateMovingAverage(currentProduceRate, historicProduceRates[dayOfWeek][hourOfDay]);
        } else {
          historicProduceRates[dayOfWeek][hourOfDay] = currentProduceRate;
        }
      } else {
        //No previous values found. Bootstrap with the average rate computed in the current window.
        historicProduceRates = new double[7][24];
        for (double[] row : historicProduceRates) {
          Arrays.fill(row, -1.0);
        }
        historicProduceRates[dayOfWeek][hourOfDay] = currentProduceRate;
      }
    }

    double consumeRate = lastCommittedWatermark.getAvgConsumeRate();
    ingestionLatencies.add(this.statsTracker.getMaxIngestionLatency(TimeUnit.SECONDS));
    consumptionRateMBps.add(this.statsTracker.getConsumptionRateMBps());

    if (isConsumerBacklogged()) {
      //If ingestion latency is high, it means the consumer is backlogged. Hence, its current consumption rate
      //must equal the peak consumption rate.
      consumeRate = consumeRate >= 0 ? updateMovingAverage(getPenultimateElement(consumptionRateMBps), consumeRate)
          : this.statsTracker.getConsumptionRateMBps();
    }
    return new TopicPartitionStats(historicProduceRates, consumeRate, newAvgRecordSize, currentProduceRate);
  }

  private boolean isConsumerBacklogged() {
    if (this.ingestionLatencies.size() < SLIDING_WINDOW_SIZE) {
      return false;
    }
    for (long latency: this.ingestionLatencies) {
      if (latency < (2 * this.flushIntervalSecs)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the element before the final element. It does this by removing the oldest element and peeking at the
   * next element.
   * @param queue
   * @return
   */
  static Double getPenultimateElement(EvictingQueue<Double> queue) {
    Preconditions.checkArgument(queue.size() > 1);
    queue.remove();
    return queue.peek();
  }

  /**
   * A method that computes a new moving average from previous average estimate and current value using an
   * Exponentially weighted moving average (EWMA) algorithm.
   * @param currentValue
   * @param previousAverage
   * @return updated moving average computed as an EWMA.
   */
  private Double updateMovingAverage(double currentValue, double previousAverage) {
    return exponentialDecayFactor * currentValue + (1 - exponentialDecayFactor) * previousAverage;
  }

  /**
   * Several side effects in this method:
   * 1. Write ProduceRate of each KafkaPartition into its watermark as the method name indicates.
   * 2. Update {@link #partitionsToProdRate} for each KafkaPartitions with their newest ProduceRate, this would be
   * part of GTE to be emitted as each flush happens.
   */
  public void writeProduceRateToKafkaWatermarks(Map<KafkaPartition, Long> latestOffsetMap, Map<String, CheckpointableWatermark> lastCommittedWatermarks,
      MultiLongWatermark highWatermark, long currentTimeMillis) {
    int partitionIndex = 0;
    Map<String, CheckpointableWatermark> unacknowledgedWatermarks = watermarkTracker.getAllUnacknowledgedWatermarks();
    for (KafkaPartition partition : this.partitionsToProdRate.keySet()) {
      long maxOffset = latestOffsetMap.getOrDefault(partition, -1L);

      KafkaStreamingExtractor.KafkaWatermark kafkaWatermark =
          (KafkaStreamingExtractor.KafkaWatermark) lastCommittedWatermarks.get(partition.toString());
      KafkaStreamingExtractor.KafkaWatermark unacknowledgedWatermark =
          (KafkaStreamingExtractor.KafkaWatermark) unacknowledgedWatermarks.get(partition.toString());

      if (kafkaWatermark == null) {
        //If there is no previously committed watermark for the topic partition, create a dummy watermark for computing stats
        kafkaWatermark = new KafkaStreamingExtractor.KafkaWatermark(partition, new LongWatermark(maxOffset >= 0? maxOffset : 0L));
      }
      long avgRecordSize = this.statsTracker.getAvgRecordSize(partitionIndex);
      long previousMaxOffset = highWatermark.get(partitionIndex++);
      //If maxOffset < 0, it means that we could not get max offsets from Kafka due to metadata fetch failure.
      // In this case, carry previous state forward and set produce-rate to negative value, indicating it's not available.
      TopicPartitionStats stats =
          maxOffset >= 0 ? getNewTopicPartitionStats(previousMaxOffset, maxOffset, kafkaWatermark, currentTimeMillis,
              avgRecordSize)
              : new TopicPartitionStats(kafkaWatermark.getAvgProduceRates(), kafkaWatermark.getAvgConsumeRate(), kafkaWatermark.getAvgRecordSize(), -1);

      if (unacknowledgedWatermark == null) {
        //If no record seen for this topicPartition in the current time window; carry forward the previously committed
        // watermark with the updated statistics
        unacknowledgedWatermark = kafkaWatermark;
        watermarkTracker.unacknowledgedWatermark(unacknowledgedWatermark);
      }
      unacknowledgedWatermark.setAvgProduceRates(stats.getAvgProduceRates());
      unacknowledgedWatermark.setAvgConsumeRate(stats.getAvgConsumeRate());
      unacknowledgedWatermark.setAvgRecordSize(stats.getAvgRecordSize());
      partitionsToProdRate.put(partition, stats.getCurrentProduceRate());
    }
    this.lastReportTimeMillis = currentTimeMillis;
  }

  /**
   * @param date
   * @return true if:
   *  <ul>
   *    <li>Stats collection on holidays is enabled</li>, or
   *    <li>{@param date} is a holiday for the given locale</li>
   *  </ul>
   */
  boolean isHoliday(LocalDate date) {
    if (!this.disableStatsOnHolidays) {
      return false;
    }
    if (holidayMap.containsKey(date)) {
      return holidayMap.get(date);
    } else {
      boolean isHolidayToday = this.holidayManager.isHoliday(date, this.holidayLocale);
      holidayMap.put(date, isHolidayToday);
      return isHolidayToday;
    }
  }
}