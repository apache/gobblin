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
package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;


import java.util.Date;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaProduceRateTracker;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;


/**
 * A {@link KafkaWorkUnitSizeEstimator} that uses historic produce rates of Kafka TopicPartitions and the current lag
 * to determine the Workunit size. The inputs to the WorkUnitSizeEstimator are the following:
 * <ul>
 *   <li> Current Lag in number of records (L) </li>
 *   <li> Average record size (R) </li>
 *   <li> Historic produce rates by hour-of-day and day-of-week (P) </li>
 *   <li> Target SLA to achieve zero lag (SLA) </li>
 * </ul>
 * Based on the current lag, historic produce rate for the Kafka TopicPartition, and
 * a target SLA, we estimate the minimum consume rate (C) required to meet the target SLA using the following formula:
 * C = (L * R)/SLA + P.
 * To allow headroom for week-over-week and intra-hour variances, we scale the historic produce rates by
 * an over-provisioning factor O. The formula is then modified to:
 * C = (L * R)/SLA + (P * O).
 * The calculated consumption rate C is returned as the estimated workunit size. Note that the estimated workunit size may exceed the
 * container capacity. The bin packer is assumed to create a new bin containing only this workunit.
 *
 * Assumptions:
 * <ul>
 *   <li>The container capacity is assumed to be defined in MB/s</li>
 *   <li>The topic partition produce rates are assumed to be tracked in bytes/s</li>
 * </ul>
 */
@Slf4j
public class ProduceRateAndLagBasedWorkUnitSizeEstimator implements KafkaWorkUnitSizeEstimator {
  public static final String CATCHUP_SLA_IN_HOURS_KEY = "gobblin.kafka.catchUpSlaInHours";
  private static final int DEFAULT_CATCHUP_SLA_IN_HOURS = 1;
  //The interval at which workunits are re-calculated.
  public static final String REPLANNING_INTERVAL_IN_HOURS_KEY = "gobblin.kafka.replanningIntervalInHours";
  //Set default mode to disable time-based replanning i.e. set replanningIntervalInHours to its maximum value.
  // In this case, the work unit weight estimation will select the maximum produce rate for the topic partition across
  // all hours and days of week.
  private static final int DEFAULT_KAFKA_REPLANNING_INTERVAL = 168; //24 * 7
  //An over-provisioning factor to provide head room to allow variances in traffic e.g. sub-hour rate variances, week-over-week
  // traffic variances etc. This config has the effect of multiplying the historic rate by this factor.
  public static final String PRODUCE_RATE_SCALING_FACTOR_KEY = "gobblin.kafka.produceRateScalingFactor";
  private static final Double DEFAULT_PRODUCE_RATE_SCALING_FACTOR = 1.3;

  public static final int ONE_MEGA_BYTE = 1048576;
  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);

  private final long catchUpSlaInHours;
  private final long replanIntervalInHours;
  private final double produceRateScalingFactor;

  public ProduceRateAndLagBasedWorkUnitSizeEstimator(SourceState state) {
    this.catchUpSlaInHours = state.getPropAsLong(CATCHUP_SLA_IN_HOURS_KEY, DEFAULT_CATCHUP_SLA_IN_HOURS);
    this.replanIntervalInHours =
        state.getPropAsLong(REPLANNING_INTERVAL_IN_HOURS_KEY, DEFAULT_KAFKA_REPLANNING_INTERVAL);
    this.produceRateScalingFactor =
        state.getPropAsDouble(PRODUCE_RATE_SCALING_FACTOR_KEY, DEFAULT_PRODUCE_RATE_SCALING_FACTOR);
  }

  @Override
  public double calcEstimatedSize(WorkUnit workUnit) {
    KafkaStreamingExtractor.KafkaWatermark watermark =
        GSON.fromJson(workUnit.getProp(KafkaTopicGroupingWorkUnitPacker.PARTITION_WATERMARK),
            KafkaStreamingExtractor.KafkaWatermark.class);
    String topic = workUnit.getProp(KafkaSource.TOPIC_NAME);
    String partition = workUnit.getProp(KafkaSource.PARTITION_ID);

    double[][] avgProduceRates = null;
    long avgRecordSize = 0L;
    long offsetLag = Long.parseLong(workUnit.getProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY));

    if (watermark != null) {
      avgProduceRates = watermark.getAvgProduceRates();
      avgRecordSize = watermark.getAvgRecordSize();
      offsetLag = offsetLag - watermark.getLwm().getValue();
    } else {
      offsetLag = 0L;
    }

    double maxProduceRate = getMaxProduceRateUntilNextReplan(avgProduceRates,
        workUnit.getPropAsLong(KafkaTopicGroupingWorkUnitPacker.PACKING_START_TIME_MILLIS));

    if (maxProduceRate < 0) {
      //No previous estimates found.
      log.debug("No previous produce rate estimate found for {}", topic + "-" + partition);
      maxProduceRate = workUnit.getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.DEFAULT_WORKUNIT_SIZE_KEY);
    }

    double minWorkUnitSize = workUnit.getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.MIN_WORKUNIT_SIZE_KEY, 0.0);

    //Compute the target consume rate in MB/s.
    double targetConsumeRate =
        ((double) (offsetLag * avgRecordSize) / (catchUpSlaInHours * 3600 * ONE_MEGA_BYTE)) + (maxProduceRate
            * produceRateScalingFactor);

    log.info("TopicPartiton: {}, Max produce rate: {}, Offset lag: {}, Avg Record size: {}, Target Consume Rate: {}, Min Workunit size: {}",
        topic + ":" + partition, maxProduceRate, offsetLag, avgRecordSize, targetConsumeRate, minWorkUnitSize);
    //Return the target consumption rate to catch up with incoming traffic and current lag, as the workunit size.
    return Math.max(targetConsumeRate, minWorkUnitSize);
  }

  /**
   * @param avgProduceRates
   * @param packingTimeMillis
   * @return the maximum produce rate in MB/s observed within the time window of [packingTimeMillis, packingTimeMillis+catchUpSla].
   */
  private double getMaxProduceRateUntilNextReplan(double[][] avgProduceRates, long packingTimeMillis) {
    int dayOfWeek = KafkaProduceRateTracker.getDayOfWeek(new Date(packingTimeMillis));
    int hourOfDay = KafkaProduceRateTracker.getHourOfDay(new Date(packingTimeMillis));

    if (avgProduceRates == null) {
      return -1.0;
    }

    double max = avgProduceRates[dayOfWeek][hourOfDay];
    for (int i = 0; i < replanIntervalInHours; i++) {
      if ((hourOfDay + 1) >= 24) {
        dayOfWeek = (dayOfWeek + 1) % 7;
      }
      hourOfDay = (hourOfDay + 1) % 24;
      if (max < avgProduceRates[dayOfWeek][hourOfDay]) {
        max = avgProduceRates[dayOfWeek][hourOfDay];
      }
    }
    return max / ONE_MEGA_BYTE;
  }
}