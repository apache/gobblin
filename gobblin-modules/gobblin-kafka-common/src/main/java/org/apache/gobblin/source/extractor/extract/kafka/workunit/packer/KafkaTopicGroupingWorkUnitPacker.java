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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.CheckpointableWatermarkState;
import org.apache.gobblin.runtime.StateStoreBasedWatermarkStorage;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaUtils;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;

import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaBiLevelWorkUnitPacker.bestFitDecreasingBinPacking;

/**
 *
 * An implementation of {@link KafkaWorkUnitPacker} that used for streamlined Kafka ingestion, which:
 *
 * 1) Groups partitions of the same topic together. Multiple topics are never mixed in a base {@link WorkUnit},
 * but may be mixed in a {@link MultiWorkUnit}
 * 2) Don't assign offset range within WorkUnit but provides a list of partitions (or topics) to inform streaming
 * {@link org.apache.gobblin.source.extractor.Extractor} of where to pull events from, behaves as an "index' for
 * {@link org.apache.gobblin.source.extractor.Extractor}.
 *
 * It is then {@link org.apache.gobblin.source.extractor.Extractor}'s responsibility to interact with
 * {@link org.apache.gobblin.writer.WatermarkStorage} on determining offset of each
 * {@link org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition} that it was assigned.
 */
@Slf4j
public class KafkaTopicGroupingWorkUnitPacker extends KafkaWorkUnitPacker {
  public static final String GOBBLIN_KAFKA_PREFIX = "gobblin.kafka.";
  private static final int DEFAULT_NUM_TOPIC_PARTITIONS_PER_CONTAINER = 10;

  //A global configuration for container capacity. The container capacity refers to the peak rate (in MB/s) that a
  //single JVM can consume from Kafka for a single topic and controls the number of partitions of a topic that will be
  // packed into a single workunit. For example, if the container capacity is set to 10, and each topic partition has a
  // weight of 1, then 10 partitions of the topic will be packed into a single workunit. This configuration is topic-independent
  // i.e. all topics will be assumed to have the same peak consumption rate when set.
  public static final String CONTAINER_CAPACITY_KEY = GOBBLIN_KAFKA_PREFIX + "streaming.containerCapacity";
  public static final double DEFAULT_CONTAINER_CAPACITY = 10;

  // minimum container capacity to avoid bad topic schema causing us to request resources aggressively
  public static final String MINIMUM_CONTAINER_CAPACITY = GOBBLIN_KAFKA_PREFIX + "streaming.minimum.containerCapacity";
  public static final double DEFAULT_MINIMUM_CONTAINER_CAPACITY = 1;
  public static final String TOPIC_PARTITION_WITH_LOW_CAPACITY_EVENT_NAME = "topicPartitionWithLowCapacity";
  public static final String TOPIC_PARTITION = "topicPartition";
  public static final String TOPIC_PARTITION_CAPACITY = "topicPartitionCapacity";

  //A boolean flag to enable per-topic container capacity, where "container capacity" is as defined earlier. This
  // configuration is useful in scenarios where the write performance can vary significantly across topics due to differences
  // in schema, as in the case of columnar formats such as ORC and Parquet. When enabled, the bin packing algorithm uses
  // historic consumption rates for a given topic as tracked by the ingestion pipeline.
  public static final String IS_PER_TOPIC_CONTAINER_CAPACITY_ENABLED_KEY = GOBBLIN_KAFKA_PREFIX + "streaming.isPerTopicBinCapacityEnabled";
  public static final Boolean DEFAULT_IS_PER_TOPIC_CONTAINER_CAPACITY_ENABLED = false;

  //A topic-specific config that controls the minimum number of containers for that topic.
  public static final String MIN_CONTAINERS_FOR_TOPIC = GOBBLIN_KAFKA_PREFIX + "minContainersForTopic";
  public static final String PARTITION_WATERMARK = GOBBLIN_KAFKA_PREFIX + "partition.watermark";

  public static final String PACKING_START_TIME_MILLIS = GOBBLIN_KAFKA_PREFIX + "packer.packingStartTimeMillis";
  public static final String IS_STATS_BASED_PACKING_ENABLED_KEY = GOBBLIN_KAFKA_PREFIX + "streaming.isStatsBasedPackingEnabled";
  public static final boolean DEFAULT_IS_STATS_BASED_PACKING_ENABLED = false;
  public static final String CONTAINER_CAPACITY_COMPUTATION_STRATEGY_KEY =
      GOBBLIN_KAFKA_PREFIX + "streaming.containerCapacityComputationStrategy";
  public static final String DEFAULT_CONTAINER_CAPACITY_COMPUTATION_STRATEGY = ContainerCapacityComputationStrategy.MEDIAN.name();

  public enum ContainerCapacityComputationStrategy {
    MIN, MAX, MEAN, MEDIAN
  }

  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);

  /**
   * Configuration to enable indexing on packing.
   */
  private static final String INDEXING_ENABLED = "gobblin.kafka.streaming.enableIndexing";
  private static final boolean DEFAULT_INDEXING_ENABLED = true;

  private static final String METRICS_PREFIX = "binpacker.metrics.";

  /**
   * When indexing-packing is enabled, the number of partitions is important for extractor to know
   * how many kafka partitions need to be pulled from.
   * Set to public-static to share with Extractor.
   */
  public static final String NUM_PARTITIONS_ASSIGNED = "gobblin.kafka.streaming.numPartitions";
  //A derived metric that defines the default workunit size, in case of workunit size cannot be estimated.
  public static final String DEFAULT_WORKUNIT_SIZE_KEY = "gobblin.kafka.defaultWorkUnitSize";
  //A lower bound for the workunit size.
  public static final String MIN_WORKUNIT_SIZE_KEY = "gobblin.kafka.minWorkUnitSize";

  private static final String NUM_CONTAINERS_EVENT_NAME = "NumContainers";

  private final long packingStartTimeMillis;
  private final double minimumContainerCapacity;
  private final Optional<StateStoreBasedWatermarkStorage> watermarkStorage;
  private final Optional<MetricContext> metricContext;
  private final boolean isStatsBasedPackingEnabled;
  private final Boolean isPerTopicContainerCapacityEnabled;
  private final ContainerCapacityComputationStrategy containerCapacityComputationStrategy;
  private final Map<String, KafkaStreamingExtractor.KafkaWatermark> lastCommittedWatermarks = Maps.newHashMap();
  private final Map<String, List<Double>> capacitiesByTopic = Maps.newHashMap();
  private final EventSubmitter eventSubmitter;
  private SourceState state;

  public KafkaTopicGroupingWorkUnitPacker(AbstractSource<?, ?> source, SourceState state,
      Optional<MetricContext> metricContext) {
    super(source, state);
    this.state = state;
    this.minimumContainerCapacity = state.getPropAsDouble(MINIMUM_CONTAINER_CAPACITY, DEFAULT_MINIMUM_CONTAINER_CAPACITY);
    this.isStatsBasedPackingEnabled =
        state.getPropAsBoolean(IS_STATS_BASED_PACKING_ENABLED_KEY, DEFAULT_IS_STATS_BASED_PACKING_ENABLED);
    this.isPerTopicContainerCapacityEnabled = state
        .getPropAsBoolean(IS_PER_TOPIC_CONTAINER_CAPACITY_ENABLED_KEY, DEFAULT_IS_PER_TOPIC_CONTAINER_CAPACITY_ENABLED);
    this.containerCapacityComputationStrategy = ContainerCapacityComputationStrategy.valueOf(
        state.getProp(CONTAINER_CAPACITY_COMPUTATION_STRATEGY_KEY, DEFAULT_CONTAINER_CAPACITY_COMPUTATION_STRATEGY)
            .toUpperCase());
    this.watermarkStorage =
        Optional.fromNullable(this.isStatsBasedPackingEnabled ? new StateStoreBasedWatermarkStorage(state) : null);
    this.packingStartTimeMillis = System.currentTimeMillis();
    this.metricContext = metricContext;
    this.eventSubmitter =
        new EventSubmitter.Builder(this.metricContext, KafkaTopicGroupingWorkUnitPacker.class.getName()).build();
  }

  /**
   * Pack using the following strategy.
   * - Each container has a configured capacity in terms of the cost metric.
   *   This is configured by {@value CONTAINER_CAPACITY_KEY}.
   * - For each topic pack the workunits into a set of topic specific buckets by filling the fullest bucket that can hold
   *   the workunit without exceeding the container capacity.
   * - The topic specific multi-workunits are squeezed and returned as a workunit.
   */
  @Override
  public List<WorkUnit> pack(Map<String, List<WorkUnit>> workUnitsByTopic, int numContainers) {
    double containerCapacity = this.state.getPropAsDouble(CONTAINER_CAPACITY_KEY, DEFAULT_CONTAINER_CAPACITY);

    if (this.watermarkStorage.isPresent()) {
      try {
        addStatsToWorkUnits(workUnitsByTopic);
      } catch (IOException e) {
        log.error("Unable to get stats from watermark storage.");
        throw new RuntimeException(e);
      }
    }

    setWorkUnitEstSizes(workUnitsByTopic);

    List<MultiWorkUnit> mwuGroups = Lists.newArrayList();

    for (Map.Entry<String, List<WorkUnit>> entry : workUnitsByTopic.entrySet()) {
      String topic = entry.getKey();
      List<WorkUnit> workUnitsForTopic = entry.getValue();
      if (this.isStatsBasedPackingEnabled && this.isPerTopicContainerCapacityEnabled) {
        containerCapacity = getContainerCapacityForTopic(capacitiesByTopic.get(topic), this.containerCapacityComputationStrategy);
        log.info("Container capacity for topic {}: {}", topic, containerCapacity);
      }
      //Add CONTAINER_CAPACITY into each workunit. Useful when KafkaIngestionHealthCheck is enabled.
      for (WorkUnit workUnit: workUnitsForTopic) {
        workUnit.setProp(CONTAINER_CAPACITY_KEY, containerCapacity);
      }
      double estimatedDataSizeForTopic = calcTotalEstSizeForTopic(workUnitsForTopic);
      int previousSize = mwuGroups.size();
      if (estimatedDataSizeForTopic < containerCapacity) {
        // If the total estimated size of a topic is then the container capacity then put all partitions of this
        // topic in a single group.
        MultiWorkUnit mwuGroup = MultiWorkUnit.createEmpty();
        addWorkUnitsToMultiWorkUnit(workUnitsForTopic, mwuGroup);
        mwuGroups.add(mwuGroup);
      } else {
        // Use best-fit-decreasing to group workunits for a topic into multiple groups.
        mwuGroups.addAll(bestFitDecreasingBinPacking(workUnitsForTopic, containerCapacity));
      }
      int numContainersForTopic = mwuGroups.size() - previousSize;
      log.info("Packed partitions for topic {} into {} containers", topic, Integer.toString(numContainersForTopic));
      if (this.metricContext.isPresent()) {
        //Report the number of containers used for each topic.
        String metricName = METRICS_PREFIX + topic + ".numContainers";
        ContextAwareGauge<Integer> gauge =
            this.metricContext.get().newContextAwareGauge(metricName, () -> numContainersForTopic);
        this.metricContext.get().register(metricName, gauge);

        //Submit a container count event for the given topic
        CountEventBuilder countEventBuilder = new CountEventBuilder(NUM_CONTAINERS_EVENT_NAME, numContainersForTopic);
        countEventBuilder.addMetadata("topic", topic);
        this.eventSubmitter.submit(countEventBuilder);
      }
    }

    List<WorkUnit> squeezedGroups = squeezeMultiWorkUnits(mwuGroups);
    log.debug("Squeezed work unit groups: " + squeezedGroups);
    return squeezedGroups;
  }


  /**
   * TODO: This method should be moved into {@link KafkaSource}, which requires moving classes such
   * as {@link KafkaStreamingExtractor.KafkaWatermark} to the open source. A side-effect of this method is to
   * populate a map (called "capacitiesByTopic") of topicName to the peak consumption rate observed
   * by a JVM for a given topic. This capacity limits the number of partitions of a topic grouped into a single workunit.
   * The capacity is computed from the historic peak consumption rates observed by different containers processing
   * a given topic, using the configured {@link ContainerCapacityComputationStrategy}.
   *
   * Read the average produce rates for each topic partition from Watermark storage and add them to the workunit.
   * @param workUnitsByTopic
   * @throws IOException
   */
  private void addStatsToWorkUnits(Map<String, List<WorkUnit>> workUnitsByTopic) throws IOException {
    for (CheckpointableWatermarkState state : this.watermarkStorage.get().getAllCommittedWatermarks()) {
      String topicPartition = state.getSource();
      KafkaStreamingExtractor.KafkaWatermark watermark =
          GSON.fromJson(state.getProp(topicPartition), KafkaStreamingExtractor.KafkaWatermark.class);
      lastCommittedWatermarks.put(topicPartition, watermark);
      if (this.isPerTopicContainerCapacityEnabled) {
        String topicName = KafkaUtils.getTopicNameFromTopicPartition(topicPartition);
        List<Double> capacities = capacitiesByTopic.getOrDefault(topicName, Lists.newArrayList());
        double realCapacity = watermark.getAvgConsumeRate() > 0 ? watermark.getAvgConsumeRate() : DEFAULT_CONTAINER_CAPACITY;
        if (realCapacity < minimumContainerCapacity) {
          if (this.metricContext.isPresent()) {
            GobblinEventBuilder event = new GobblinEventBuilder(TOPIC_PARTITION_WITH_LOW_CAPACITY_EVENT_NAME);
            event.addMetadata(TOPIC_PARTITION, topicPartition);
            event.addMetadata(TOPIC_PARTITION_CAPACITY, String.valueOf(realCapacity));
            this.eventSubmitter.submit(event);
          }
          log.warn(String.format("topicPartition %s has lower capacity %s, ignore that and reset capacity to be %s", topicPartition, realCapacity, minimumContainerCapacity));
          realCapacity = minimumContainerCapacity;
        }
        capacities.add(realCapacity);
        capacitiesByTopic.put(topicName, capacities);
      }
    }

    for (Map.Entry<String, List<WorkUnit>> entry : workUnitsByTopic.entrySet()) {
      String topic = entry.getKey();
      List<WorkUnit> workUnits = entry.getValue();
      for (WorkUnit workUnit : workUnits) {
        int partitionId = Integer.parseInt(workUnit.getProp(KafkaSource.PARTITION_ID));
        String topicPartition = new KafkaPartition.Builder().withTopicName(topic).withId(partitionId).build().toString();
        KafkaStreamingExtractor.KafkaWatermark watermark = lastCommittedWatermarks.get(topicPartition);
        workUnit.setProp(PARTITION_WATERMARK, GSON.toJson(watermark));
        workUnit.setProp(PACKING_START_TIME_MILLIS, this.packingStartTimeMillis);
        workUnit.setProp(DEFAULT_WORKUNIT_SIZE_KEY, getDefaultWorkUnitSize());
        workUnit.setProp(MIN_WORKUNIT_SIZE_KEY, getMinWorkUnitSize(workUnit));
        // avgRecordSize is unknown when bootstrapping. so skipping setting this
        // and ORC writer will use the default setting for the tunning feature.
        if (watermark != null && watermark.getAvgRecordSize() > 0) {
          workUnit.setProp(ConfigurationKeys.AVG_RECORD_SIZE, watermark.getAvgRecordSize());
        }
      }
    }
  }

  private Double getDefaultWorkUnitSize() {
    return state.getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY,
        KafkaTopicGroupingWorkUnitPacker.DEFAULT_CONTAINER_CAPACITY) / DEFAULT_NUM_TOPIC_PARTITIONS_PER_CONTAINER;
  }

  /**
   * A helper method that configures the minimum workunit size for each topic partition based on the lower bound of
   * the number of containers to be used for the topic.
   * @param workUnit
   * @return the minimum workunit size.
   */
  private Double getMinWorkUnitSize(WorkUnit workUnit) {
    int minContainersForTopic = Math.min(workUnit.getPropAsInt(MIN_CONTAINERS_FOR_TOPIC, -1),
        workUnit.getPropAsInt(KafkaSource.NUM_TOPIC_PARTITIONS));
    if (minContainersForTopic == -1) {
      //No minimum configured? Return lower bound for workunit size to be 0.
      return 0.0;
    }

    //Compute the maximum number of partitions to be packed into each container.
    int maxNumPartitionsPerContainer = workUnit.getPropAsInt(KafkaSource.NUM_TOPIC_PARTITIONS) / minContainersForTopic;
    return state.getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY,
        KafkaTopicGroupingWorkUnitPacker.DEFAULT_CONTAINER_CAPACITY) / maxNumPartitionsPerContainer;
  }

  /**
   * Indexing all partitions that will be handled in this topic-specific MWU into a single WU by only tracking their
   * topic/partition id.
   * Indexed WU will not have offset assigned to pull for each partitions
   */
  @Override
  protected List<WorkUnit> squeezeMultiWorkUnits(List<MultiWorkUnit> multiWorkUnits) {

    if (state.getPropAsBoolean(INDEXING_ENABLED, DEFAULT_INDEXING_ENABLED)) {
      List<WorkUnit> indexedWorkUnitList = new ArrayList<>();

      // id to append to the task output directory to make it unique to avoid multiple flush publishers
      // attempting to move the same file.
      int uniqueId = 0;
      for (MultiWorkUnit mwu : multiWorkUnits) {
        // Select a sample WU.
        WorkUnit indexedWorkUnit = mwu.getWorkUnits().get(0);
        List<KafkaPartition> topicPartitions = getPartitionsFromMultiWorkUnit(mwu);

        // Indexing all topics/partitions into this WU.
        populateMultiPartitionWorkUnit(topicPartitions, indexedWorkUnit);

        // Adding Number of Partitions as part of WorkUnit so that Extractor has clue on how many iterations to run.
        indexedWorkUnit.setProp(NUM_PARTITIONS_ASSIGNED, topicPartitions.size());

        // Need to make the task output directory unique to file move conflicts in the flush publisher.
        String outputDir = state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR);
        indexedWorkUnit.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(outputDir, Integer.toString(uniqueId++)));
        indexedWorkUnitList.add(indexedWorkUnit);
      }
      return indexedWorkUnitList;
    } else {
      return super.squeezeMultiWorkUnits(multiWorkUnits);
    }
  }

  /**
   * A method that returns the container capacity for a given topic given the
   * @param capacities measured container capacities derived from watermarks
   * @param strategy the algorithm to derive the container capacity from prior measurements
   * @return the container capacity obtained using the given {@link ContainerCapacityComputationStrategy}.
   */
  @VisibleForTesting
  static double getContainerCapacityForTopic(List<Double> capacities, ContainerCapacityComputationStrategy strategy) {
    //No previous stats for a topic? Return default.
    if (capacities == null) {
      return DEFAULT_CONTAINER_CAPACITY;
    }
    Collections.sort(capacities);
    log.info("Capacity computation strategy: {}, capacities: {}", strategy.name(), capacities);
    switch (strategy) {
      case MIN:
        return capacities.get(0);
      case MAX:
        return capacities.get(capacities.size() - 1);
      case MEAN:
        return (capacities.stream().mapToDouble(capacity -> capacity).sum()) / capacities.size();
      case MEDIAN:
        return ((capacities.size() % 2) == 0) ? (
            (capacities.get(capacities.size() / 2) + capacities.get(capacities.size() / 2 - 1)) / 2)
            : capacities.get(capacities.size() / 2);
      default:
        throw new RuntimeException("Unsupported computation strategy: " + strategy.name());
    }
  }
}