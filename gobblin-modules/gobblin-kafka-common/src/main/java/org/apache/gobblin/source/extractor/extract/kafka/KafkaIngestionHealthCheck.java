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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.EvictingQueue;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;


@Slf4j
@Alias(value = "KafkaIngestionHealthCheck")
public class KafkaIngestionHealthCheck implements CommitStep {
  public static final String KAFKA_INGESTION_HEALTH_CHECK_PREFIX = "gobblin.kafka.healthCheck.";
  public static final String KAFKA_INGESTION_HEALTH_CHECK_SLIDING_WINDOW_SIZE_KEY = KAFKA_INGESTION_HEALTH_CHECK_PREFIX + "slidingWindow.size";
  public static final String KAFKA_INGESTION_HEALTH_CHECK_LATENCY_THRESHOLD_MINUTES_KEY = KAFKA_INGESTION_HEALTH_CHECK_PREFIX + "ingestionLatency.minutes";
  public static final String KAFKA_INGESTION_HEALTH_CHECK_CONSUMPTION_RATE_DROPOFF_FRACTION_KEY = KAFKA_INGESTION_HEALTH_CHECK_PREFIX + "consumptionRate.dropOffFraction";
  public static final String KAFKA_INGESTION_HEALTH_CHECK_INCREASING_LATENCY_CHECK_ENABLED_KEY = KAFKA_INGESTION_HEALTH_CHECK_PREFIX + "increasingLatencyCheckEnabled";

  public static final int DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_SLIDING_WINDOW_SIZE = 3;
  public static final long DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_LATENCY_THRESHOLD_MINUTES= 15;
  public static final double DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_CONSUMPTION_RATE_DROPOFF_FRACTION = 0.7;
  private static final boolean DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_INCREASING_LATENCY_CHECK_ENABLED = true;

  private final Config config;
  private final EventBus eventBus;
  private final KafkaExtractorStatsTracker statsTracker;
  private final double expectedConsumptionRate;
  private final double consumptionRateDropOffFraction;
  private final long ingestionLatencyThresholdMinutes;
  private final int slidingWindowSize;
  private final EvictingQueue<Long> ingestionLatencies;
  private final EvictingQueue<Double> consumptionRateMBps;
  private final boolean increasingLatencyCheckEnabled;

  public KafkaIngestionHealthCheck(Config config, KafkaExtractorStatsTracker statsTracker) {
    this.config = config;
    this.slidingWindowSize = ConfigUtils.getInt(config, KAFKA_INGESTION_HEALTH_CHECK_SLIDING_WINDOW_SIZE_KEY, DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_SLIDING_WINDOW_SIZE);
    this.ingestionLatencyThresholdMinutes = ConfigUtils.getLong(config, KAFKA_INGESTION_HEALTH_CHECK_LATENCY_THRESHOLD_MINUTES_KEY, DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_LATENCY_THRESHOLD_MINUTES);
    this.consumptionRateDropOffFraction = ConfigUtils.getDouble(config, KAFKA_INGESTION_HEALTH_CHECK_CONSUMPTION_RATE_DROPOFF_FRACTION_KEY, DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_CONSUMPTION_RATE_DROPOFF_FRACTION);
    this.expectedConsumptionRate = ConfigUtils.getDouble(config, KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY, KafkaTopicGroupingWorkUnitPacker.DEFAULT_CONTAINER_CAPACITY);
    this.increasingLatencyCheckEnabled = ConfigUtils.getBoolean(config, KAFKA_INGESTION_HEALTH_CHECK_INCREASING_LATENCY_CHECK_ENABLED_KEY, DEFAULT_KAFKA_INGESTION_HEALTH_CHECK_INCREASING_LATENCY_CHECK_ENABLED);
    this.ingestionLatencies = EvictingQueue.create(this.slidingWindowSize);
    this.consumptionRateMBps = EvictingQueue.create(this.slidingWindowSize);
    EventBus eventBus;
    try {
      eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
          SharedResourcesBrokerFactory.getImplicitBroker());
    } catch (IOException e) {
      log.error("Could not find EventBus instance for container health check", e);
      eventBus = null;
    }
    this.eventBus = eventBus;
    this.statsTracker = statsTracker;
  }

  /**
   *
   * @return true if (i) ingestionLatency in the each of the recent epochs exceeds the threshold latency , AND (ii)
   * if {@link KafkaIngestionHealthCheck#increasingLatencyCheckEnabled} is true, the latency
   * is increasing over these epochs.
   */
  private boolean checkIngestionLatency() {
    Long previousLatency = -1L;
    for (Long ingestionLatency: ingestionLatencies) {
      if (ingestionLatency < this.ingestionLatencyThresholdMinutes) {
        return false;
      } else {
        if (this.increasingLatencyCheckEnabled) {
          if (previousLatency > ingestionLatency) {
            return false;
          }
          previousLatency = ingestionLatency;
        }
      }
    }
    return true;
  }

  /**
   * Determine whether the commit step has been completed.
   */
  @Override
  public boolean isCompleted()
      throws IOException {
    return false;
  }

  /**
   * @return Return a serialized string representation of health check report.
   */
  private String getHealthCheckReport() {
    return String.format("Ingestion Latencies = %s, Ingestion Latency Threshold = %s minutes, "
        + "Consumption Rates = %s, Target Consumption Rate = %s MBps", this.ingestionLatencies.toString(),
        this.ingestionLatencyThresholdMinutes, this.consumptionRateMBps.toString(), this.expectedConsumptionRate);
  }

  /**
   * Execute the commit step. The execute method gets the maximum ingestion latency and the consumption rate and emits
   * a {@link ContainerHealthCheckFailureEvent} if the following conditions are satisfied:
   * <li>
   *   <ul>The ingestion latency increases monotonically over the {@link KafkaIngestionHealthCheck#slidingWindowSize} intervals, AND </ul>
   *   <ul>The maximum consumption rate over the {@link KafkaIngestionHealthCheck#slidingWindowSize} intervals is smaller than
   *   {@link KafkaIngestionHealthCheck#consumptionRateDropOffFraction} * {@link KafkaIngestionHealthCheck#expectedConsumptionRate}</ul>.
   * </li>
   *
   * The {@link ContainerHealthCheckFailureEvent} is posted to a global event bus. The handlers of this event type
   * can perform suitable actions based on the execution environment.
   */
  @Override
  public void execute() {
    this.ingestionLatencies.add(this.statsTracker.getMaxIngestionLatency(TimeUnit.MINUTES));
    this.consumptionRateMBps.add(this.statsTracker.getConsumptionRateMBps());
    if (ingestionLatencies.size() < this.slidingWindowSize) {
      log.info("SUCCESS: Num observations: {} smaller than {}", ingestionLatencies.size(), this.slidingWindowSize);
      return;
    }

    if (!checkIngestionLatency()) {
      log.info("SUCCESS: Ingestion Latencies = {}, Ingestion Latency Threshold: {}", this.ingestionLatencies.toString(), this.ingestionLatencyThresholdMinutes);
      return;
    }

    double avgConsumptionRate = getMaxConsumptionRate();
    if (avgConsumptionRate > this.consumptionRateDropOffFraction * this.expectedConsumptionRate) {
      log.info("SUCCESS: Avg. Consumption Rate = {} MBps, Target Consumption rate = {} MBps", avgConsumptionRate, this.expectedConsumptionRate);
      return;
    }

    log.error("FAILED: {}", getHealthCheckReport());

    if (this.eventBus != null) {
      log.info("Posting {} message to EventBus", ContainerHealthCheckFailureEvent.class.getSimpleName());
      ContainerHealthCheckFailureEvent event = new ContainerHealthCheckFailureEvent(this.config, getClass().getName());
      event.addMetadata("ingestionLatencies", this.ingestionLatencies.toString());
      event.addMetadata("consumptionRates", this.consumptionRateMBps.toString());
      event.addMetadata("ingestionLatencyThreshold", Long.toString(this.ingestionLatencyThresholdMinutes));
      event.addMetadata("targetConsumptionRate", Double.toString(this.expectedConsumptionRate));
      this.eventBus.post(event);
    }
  }

  private double getMaxConsumptionRate() {
    return consumptionRateMBps.stream().mapToDouble(consumptionRate -> consumptionRate)
        .filter(consumptionRate -> consumptionRate >= 0.0).max().orElse(0.0);
  }
}
