/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Doubles;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaSource;
import gobblin.source.extractor.extract.kafka.KafkaUtils;
import gobblin.source.extractor.extract.kafka.MultiLongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * An abstract class for packing Kafka {@link WorkUnit}s into {@link MultiWorkUnit}s
 * based on the number of containers.
 *
 * @author Ziyang Liu
 */
public abstract class KafkaWorkUnitPacker {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWorkUnitPacker.class);

  public enum PackerType {
    SINGLE_LEVEL,
    BI_LEVEL
  }

  public enum SizeEstimatorType {
    AVG_RECORD_TIME,
    AVG_RECORD_SIZE
  }

  public static final String KAFKA_WORKUNIT_PACKER_TYPE = "kafka.workunit.packer.type";
  private static final PackerType DEFAULT_PACKER_TYPE = PackerType.SINGLE_LEVEL;

  public static final String KAFKA_WORKUNIT_SIZE_ESTIMATOR_TYPE = "kafka.workunit.size.estimator.type";
  private static final SizeEstimatorType DEFAULT_SIZE_ESTIMATOR_TYPE = SizeEstimatorType.AVG_RECORD_TIME;

  protected static final double EPS = 0.01;

  public static final String MIN_MULTIWORKUNIT_LOAD = "min.multiworkunit.load";
  public static final String MAX_MULTIWORKUNIT_LOAD = "max.multiworkunit.load";
  private static final String ESTIMATED_WORKUNIT_SIZE = "estimated.workunit.size";

  protected final AbstractSource<?, ?> source;
  protected final SourceState state;
  protected final KafkaWorkUnitSizeEstimator sizeEstimator;

  protected KafkaWorkUnitPacker(AbstractSource<?, ?> source, SourceState state) {
    this.source = source;
    this.state = state;
    this.sizeEstimator = getWorkUnitSizeEstimator();
  }

  protected static final Comparator<WorkUnit> LOAD_ASC_COMPARATOR = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Doubles.compare(getWorkUnitEstLoad(w1), getWorkUnitEstLoad(w2));
    }
  };

  protected static final Comparator<WorkUnit> LOAD_DESC_COMPARATOR = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Doubles.compare(getWorkUnitEstLoad(w2), getWorkUnitEstLoad(w1));
    }
  };

  protected double setWorkUnitEstSizes(Map<String, List<WorkUnit>> workUnitsByTopic) {
    double totalEstDataSize = 0;
    for (List<WorkUnit> workUnitsForTopic : workUnitsByTopic.values()) {
      for (WorkUnit workUnit : workUnitsForTopic) {
        setWorkUnitEstSize(workUnit);
        totalEstDataSize += getWorkUnitEstSize(workUnit);
      }
    }
    return totalEstDataSize;
  }

  private void setWorkUnitEstSize(WorkUnit workUnit) {
    workUnit.setProp(ESTIMATED_WORKUNIT_SIZE, this.sizeEstimator.calcEstimatedSize(workUnit));
  }

  private KafkaWorkUnitSizeEstimator getWorkUnitSizeEstimator() {
    if (this.state.contains(KAFKA_WORKUNIT_SIZE_ESTIMATOR_TYPE)) {
      String sizeEstimatorTypeString = this.state.getProp(KAFKA_WORKUNIT_SIZE_ESTIMATOR_TYPE);
      Optional<SizeEstimatorType> sizeEstimatorType =
          Enums.getIfPresent(SizeEstimatorType.class, sizeEstimatorTypeString);
      if (sizeEstimatorType.isPresent()) {
        return getWorkUnitSizeEstimator(sizeEstimatorType.get());
      }
      throw new IllegalArgumentException("WorkUnit size estimator type " + sizeEstimatorType + " not found");
    }
    return getWorkUnitSizeEstimator(DEFAULT_SIZE_ESTIMATOR_TYPE);
  }

  private KafkaWorkUnitSizeEstimator getWorkUnitSizeEstimator(SizeEstimatorType sizeEstimatorType) {
    switch (sizeEstimatorType) {
      case AVG_RECORD_TIME:
        return new KafkaAvgRecordTimeBasedWorkUnitSizeEstimator(this.state);
      case AVG_RECORD_SIZE:
        return new KafkaAvgRecordSizeBasedWorkUnitSizeEstimator(this.state);
      default:
        throw new IllegalArgumentException("WorkUnit size estimator type " + sizeEstimatorType + " not found");
    }
  }

  private static void setWorkUnitEstSize(WorkUnit workUnit, double estSize) {
    workUnit.setProp(ESTIMATED_WORKUNIT_SIZE, estSize);
  }

  protected static double getWorkUnitEstSize(WorkUnit workUnit) {
    Preconditions.checkArgument(workUnit.contains(ESTIMATED_WORKUNIT_SIZE));
    return workUnit.getPropAsDouble(ESTIMATED_WORKUNIT_SIZE);
  }

  protected static double getWorkUnitEstLoad(WorkUnit workUnit) {
    if (workUnit instanceof MultiWorkUnit) {
      MultiWorkUnit mwu = (MultiWorkUnit) workUnit;
      return Math.max(getWorkUnitEstSize(workUnit), EPS) * Math.log10(Math.max(mwu.getWorkUnits().size(), 2));
    }
    return Math.max(getWorkUnitEstSize(workUnit), EPS) * Math.log10(2.0);
  }

  protected static void addWorkUnitToMultiWorkUnit(WorkUnit workUnit, MultiWorkUnit multiWorkUnit) {
    multiWorkUnit.addWorkUnit(workUnit);
    double size = multiWorkUnit.getPropAsDouble(ESTIMATED_WORKUNIT_SIZE, 0.0);
    multiWorkUnit.setProp(ESTIMATED_WORKUNIT_SIZE, size + getWorkUnitEstSize(workUnit));
  }

  protected static void addWorkUnitsToMultiWorkUnit(List<WorkUnit> workUnits, MultiWorkUnit multiWorkUnit) {
    for (WorkUnit workUnit : workUnits) {
      addWorkUnitToMultiWorkUnit(workUnit, multiWorkUnit);
    }
  }

  @SuppressWarnings("deprecation")
  protected static WatermarkInterval getWatermarkIntervalFromWorkUnit(WorkUnit workUnit) {
    if (workUnit instanceof MultiWorkUnit) {
      return getWatermarkIntervalFromMultiWorkUnit((MultiWorkUnit) workUnit);
    }
    List<Long> lowWatermarkValues = Lists.newArrayList(workUnit.getLowWaterMark());
    List<Long> expectedHighWatermarkValues = Lists.newArrayList(workUnit.getHighWaterMark());
    return new WatermarkInterval(new MultiLongWatermark(lowWatermarkValues),
        new MultiLongWatermark(expectedHighWatermarkValues));
  }

  @SuppressWarnings("deprecation")
  protected static WatermarkInterval getWatermarkIntervalFromMultiWorkUnit(MultiWorkUnit multiWorkUnit) {
    List<Long> lowWatermarkValues = Lists.newArrayList();
    List<Long> expectedHighWatermarkValues = Lists.newArrayList();
    for (WorkUnit workUnit : multiWorkUnit.getWorkUnits()) {
      lowWatermarkValues.add(workUnit.getLowWaterMark());
      expectedHighWatermarkValues.add(workUnit.getHighWaterMark());
    }
    return new WatermarkInterval(new MultiLongWatermark(lowWatermarkValues),
        new MultiLongWatermark(expectedHighWatermarkValues));
  }

  /**
   * For each input {@link MultiWorkUnit}, combine all {@link WorkUnit}s in it into a single {@link WorkUnit}.
   */
  protected List<WorkUnit> squeezeMultiWorkUnits(List<MultiWorkUnit> multiWorkUnits) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (MultiWorkUnit multiWorkUnit : multiWorkUnits) {
      workUnits.add(squeezeMultiWorkUnit(multiWorkUnit));
    }
    return workUnits;
  }

  /**
   * Combine all {@link WorkUnit}s in the {@link MultiWorkUnit} into a single {@link WorkUnit}.
   */
  protected WorkUnit squeezeMultiWorkUnit(MultiWorkUnit multiWorkUnit) {
    WatermarkInterval interval = getWatermarkIntervalFromMultiWorkUnit(multiWorkUnit);
    List<KafkaPartition> partitions = getPartitionsFromMultiWorkUnit(multiWorkUnit);
    Preconditions.checkArgument(!partitions.isEmpty(), "There must be at least one partition in the multiWorkUnit");
    Extract extract = this.source.createExtract(KafkaSource.DEFAULT_TABLE_TYPE, KafkaSource.DEFAULT_NAMESPACE_NAME,
        partitions.get(0).getTopicName());
    WorkUnit workUnit = WorkUnit.create(extract, interval);
    populateMultiPartitionWorkUnit(partitions, workUnit);
    workUnit.setProp(ESTIMATED_WORKUNIT_SIZE, multiWorkUnit.getProp(ESTIMATED_WORKUNIT_SIZE));
    LOG.info(String.format("Created MultiWorkUnit for partitions %s", partitions));
    return workUnit;
  }

  /**
   * Add a list of partitions of the same topic to a {@link WorkUnit}.
   */
  private static void populateMultiPartitionWorkUnit(List<KafkaPartition> partitions, WorkUnit workUnit) {
    Preconditions.checkArgument(!partitions.isEmpty(), "There should be at least one partition");
    workUnit.setProp(KafkaSource.TOPIC_NAME, partitions.get(0).getTopicName());
    GobblinMetrics.addCustomTagToState(workUnit, new Tag<>("kafkaTopic", partitions.get(0).getTopicName()));
    workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partitions.get(0).getTopicName());
    for (int i = 0; i < partitions.size(); i++) {
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i), partitions.get(i).getId());
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_ID, i),
          partitions.get(i).getLeader().getId());
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_HOSTANDPORT, i),
          partitions.get(i).getLeader().getHostAndPort());
    }
  }

  private static List<KafkaPartition> getPartitionsFromMultiWorkUnit(MultiWorkUnit multiWorkUnit) {
    List<KafkaPartition> partitions = Lists.newArrayList();

    for (WorkUnit workUnit : multiWorkUnit.getWorkUnits()) {
      partitions.add(KafkaUtils.getPartition(workUnit));
    }

    return partitions;
  }

  /**
   * Pack a list of {@link WorkUnit}s into a smaller number of {@link MultiWorkUnit}s,
   * using the worst-fit-decreasing algorithm.
   *
   * Each {@link WorkUnit} is assigned to the {@link MultiWorkUnit} with the smallest load.
   */
  protected List<WorkUnit> worstFitDecreasingBinPacking(List<WorkUnit> groups, int numOfMultiWorkUnits) {

    // Sort workunit groups by data size desc
    Collections.sort(groups, LOAD_DESC_COMPARATOR);

    MinMaxPriorityQueue<MultiWorkUnit> pQueue =
        MinMaxPriorityQueue.orderedBy(LOAD_ASC_COMPARATOR).expectedSize(numOfMultiWorkUnits).create();
    for (int i = 0; i < numOfMultiWorkUnits; i++) {
      MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
      setWorkUnitEstSize(multiWorkUnit, 0);
      pQueue.add(multiWorkUnit);
    }

    for (WorkUnit group : groups) {
      MultiWorkUnit lightestMultiWorkUnit = pQueue.poll();
      addWorkUnitToMultiWorkUnit(group, lightestMultiWorkUnit);
      pQueue.add(lightestMultiWorkUnit);
    }

    logMultiWorkUnitInfo(pQueue);

    double minLoad = getWorkUnitEstLoad(pQueue.peekFirst());
    double maxLoad = getWorkUnitEstLoad(pQueue.peekLast());
    LOG.info(String.format("Min load of multiWorkUnit = %f; Max load of multiWorkUnit = %f; Diff = %f%%", minLoad,
        maxLoad, (maxLoad - minLoad) / maxLoad * 100.0));

    this.state.setProp(MIN_MULTIWORKUNIT_LOAD, minLoad);
    this.state.setProp(MAX_MULTIWORKUNIT_LOAD, maxLoad);

    List<WorkUnit> multiWorkUnits = Lists.newArrayList();
    multiWorkUnits.addAll(pQueue);
    return multiWorkUnits;
  }

  private static void logMultiWorkUnitInfo(Iterable<MultiWorkUnit> mwus) {
    int idx = 0;
    for (MultiWorkUnit mwu : mwus) {
      LOG.info(String.format("MultiWorkUnit %d: estimated load=%f, partitions=%s", idx++, getWorkUnitEstLoad(mwu),
          getMultiWorkUnitPartitions(mwu)));
    }
  }

  protected static List<List<KafkaPartition>> getMultiWorkUnitPartitions(MultiWorkUnit mwu) {
    List<List<KafkaPartition>> partitions = Lists.newArrayList();
    for (WorkUnit workUnit : mwu.getWorkUnits()) {
      partitions.add(KafkaUtils.getPartitions(workUnit));
    }
    return partitions;
  }

  public static KafkaWorkUnitPacker getInstance(AbstractSource<?, ?> source, SourceState state) {
    if (state.contains(KAFKA_WORKUNIT_PACKER_TYPE)) {
      String packerTypeStr = state.getProp(KAFKA_WORKUNIT_PACKER_TYPE);
      Optional<PackerType> packerType = Enums.getIfPresent(PackerType.class, packerTypeStr);
      if (packerType.isPresent()) {
        return getInstance(packerType.get(), source, state);
      }
      throw new IllegalArgumentException("WorkUnit packer type " + packerTypeStr + " not found");
    }
    return getInstance(DEFAULT_PACKER_TYPE, source, state);
  }

  public static KafkaWorkUnitPacker getInstance(PackerType packerType, AbstractSource<?, ?> source, SourceState state) {
    switch (packerType) {
      case SINGLE_LEVEL:
        return new KafkaSingleLevelWorkUnitPacker(source, state);
      case BI_LEVEL:
        return new KafkaBiLevelWorkUnitPacker(source, state);
      default:
        throw new IllegalArgumentException("WorkUnit packer type " + packerType + " not found");
    }
  }

  /**
   * Group {@link WorkUnit}s into {@link MultiWorkUnit}s. Each input {@link WorkUnit} corresponds to
   * a (topic, partition).
   */
  public abstract List<WorkUnit> pack(Map<String, List<WorkUnit>> workUnitsByTopic, int numContainers);
}
