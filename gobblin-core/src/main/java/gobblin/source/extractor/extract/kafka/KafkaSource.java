/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.EventBasedSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.DatasetFilterUtils;


/**
 * A {@link gobblin.source.Source} implementation for Kafka source.
 *
 * @author ziliu
 */
public abstract class KafkaSource<S, D> extends EventBasedSource<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
  private static final Gson GSON = new Gson();

  public static final String TOPIC_BLACKLIST = "topic.blacklist";
  public static final String TOPIC_WHITELIST = "topic.whitelist";
  public static final String LATEST_OFFSET = "latest";
  public static final String EARLIEST_OFFSET = "earliest";
  public static final String NEAREST_OFFSET = "nearest";
  public static final String BOOTSTRAP_WITH_OFFSET = "bootstrap.with.offset";
  public static final String DEFAULT_BOOTSTRAP_WITH_OFFSET = LATEST_OFFSET;
  public static final String TOPICS_MOVE_TO_LATEST_OFFSET = "topics.move.to.latest.offset";
  public static final String RESET_ON_OFFSET_OUT_OF_RANGE = "reset.on.offset.out.of.range";
  public static final String DEFAULT_RESET_ON_OFFSET_OUT_OF_RANGE = NEAREST_OFFSET;
  public static final String TOPIC_NAME = "topic.name";
  public static final String PARTITION_ID = "partition.id";
  public static final String LEADER_ID = "leader.id";
  public static final String LEADER_HOSTANDPORT = "leader.hostandport";
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "KAFKA";
  public static final String ALL_TOPICS = "all";
  public static final String AVG_EVENT_SIZE = "avg.event.size";
  public static final long DEFAULT_AVG_EVENT_SIZE = 1024;
  public static final String ESTIMATED_DATA_SIZE = "estimated.data.size";

  private static final Comparator<WorkUnit> SIZE_ASC_COMPARATOR = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Longs.compare(getWorkUnitEstSize(w1), getWorkUnitEstSize(w2));
    }
  };

  private static final Comparator<WorkUnit> SIZE_DESC_COMPARATOR = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Longs.compare(getWorkUnitEstSize(w2), getWorkUnitEstSize(w1));
    }
  };

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newHashMap();
  private final Map<KafkaPartition, Long> previousAvgSizes = Maps.newHashMap();

  private final Set<KafkaPartition> partitionsToBeProcessed = Sets.newHashSet();

  private int failToGetOffsetCount = 0;
  private int offsetTooEarlyCount = 0;
  private int offsetTooLateCount = 0;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Map<String, List<WorkUnit>> workUnits = Maps.newHashMap();
    Closer closer = Closer.create();
    try {
      KafkaWrapper kafkaWrapper = closer.register(KafkaWrapper.create(state));
      List<KafkaTopic> topics = getFilteredTopics(kafkaWrapper, state);
      for (KafkaTopic topic : topics) {
        workUnits.put(topic.getName(), getWorkUnitsForTopic(kafkaWrapper, topic, state));
      }

      // Create empty WorkUnits for skipped partitions (i.e., partitions that have previous offsets,
      // but aren't processed.
      createEmptyWorkUnitsForSkippedPartitions(workUnits, state);

      int numOfMultiWorkunits =
          state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);
      this.getAllPreviousAvgSizes(state);
      return ImmutableList.copyOf(getMultiWorkunits(workUnits, numOfMultiWorkunits, state));
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to close kafkaWrapper", e);
      }
    }
  }

  private void createEmptyWorkUnitsForSkippedPartitions(Map<String, List<WorkUnit>> workUnits, SourceState state) {

    // For each partition that has a previous offset, create an empty WorkUnit for it if
    // it is not in this.partitionsToBeProcessed.
    for (Map.Entry<KafkaPartition, Long> entry : this.previousOffsets.entrySet()) {
      KafkaPartition partition = entry.getKey();

      if (!this.partitionsToBeProcessed.contains(partition)) {
        long previousOffset = entry.getValue();
        WorkUnit emptyWorkUnit = createEmptyWorkUnit(partition, previousOffset);
        String topicName = partition.getTopicName();
        if (workUnits.containsKey(topicName)) {
          workUnits.get(topicName).add(emptyWorkUnit);
        } else {
          workUnits.put(topicName, Lists.newArrayList(emptyWorkUnit));
        }
      }
    }
  }

  /**
   * Group workUnits into multiWorkUnits. Each input workUnit corresponds to a (topic, partition).
   *
   * There are two levels of grouping. In the first level, some workunits corresponding to partitions
   * of the same topic are grouped together into a single workunit. This reduces the number of small
   * output files since these partitions will share output files, rather than each creating individual
   * output files. The number of grouped workunits is approximately 3 * numOfMultiWorkunits, since the
   * worst-fit-decreasing algorithm (used by the second level) should work well if the number of items
   * is more than 3 times the number of bins.
   *
   * In the second level, these grouped workunits are assembled into multiWorkunits using worst-fit-decreasing.
   *
   * @param workUnits A two-dimensional list of workunits, each corresponding to a single Kafka partition, grouped
   * by topics.
   * @param numOfMultiWorkunits Desired number of MultiWorkUnits.
   * @param state
   * @return A list of MultiWorkUnits.
   */
  private List<WorkUnit> getMultiWorkunits(Map<String, List<WorkUnit>> workUnits, int numOfMultiWorkunits,
      SourceState state) {
    Preconditions.checkArgument(numOfMultiWorkunits >= 1);

    long totalEstDataSize = 0;
    for (List<WorkUnit> workUnitsForTopic : workUnits.values()) {
      for (WorkUnit workUnit : workUnitsForTopic) {
        setWorkUnitEstSize(workUnit);
        totalEstDataSize += getWorkUnitEstSize(workUnit);
      }
    }
    long avgGroupSize = (long) ((double) totalEstDataSize / (double) numOfMultiWorkunits / 3.0);

    List<MultiWorkUnit> mwuGroups = Lists.newArrayList();
    for (List<WorkUnit> workUnitsForTopic : workUnits.values()) {
      long estimatedDataSizeForTopic = calcTotalEstSizeForTopic(workUnitsForTopic);
      if (estimatedDataSizeForTopic < avgGroupSize) {

        // If the total estimated size of a topic is smaller than group size, put all partitions of this
        // topic in a single group.
        MultiWorkUnit mwuGroup = new MultiWorkUnit();
        addWorkUnitsToMultiWorkUnit(workUnitsForTopic, mwuGroup);
        mwuGroups.add(mwuGroup);
      } else {

        // Use best-fit-decreasing to group workunits for a topic into multiple groups.
        mwuGroups.addAll(bestFitDecreasingBinPacking(workUnitsForTopic, avgGroupSize));
      }
    }

    List<WorkUnit> groups = squeezeMultiWorkUnits(mwuGroups, state);

    return worstFitDecreasingBinPacking(groups, numOfMultiWorkunits);
  }

  private void setWorkUnitEstSize(WorkUnit workUnit) {
    long avgSize = this.getPreviousAvgSizeForPartition(KafkaUtils.getPartition(workUnit));
    long numOfEvents = workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
        - workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    workUnit.setProp(ESTIMATED_DATA_SIZE, avgSize * numOfEvents);
  }

  private static void setWorkUnitEstSize(WorkUnit workUnit, long estSize) {
    workUnit.setProp(ESTIMATED_DATA_SIZE, estSize);
  }

  private static long getWorkUnitEstSize(WorkUnit workUnit) {
    Preconditions.checkArgument(workUnit.contains(ESTIMATED_DATA_SIZE));
    return workUnit.getPropAsLong(ESTIMATED_DATA_SIZE);
  }

  /**
   * For each input MultiWorkUnit, combine all workunits in it into a single workunit.
   */
  private List<WorkUnit> squeezeMultiWorkUnits(List<MultiWorkUnit> multiWorkUnits, SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (MultiWorkUnit multiWorkUnit : multiWorkUnits) {
      workUnits.add(squeezeMultiWorkUnit(multiWorkUnit, state));
    }
    return workUnits;
  }

  /**
   * Combine all workunits in the multiWorkUnit into a single workunit.
   */
  private WorkUnit squeezeMultiWorkUnit(MultiWorkUnit multiWorkUnit, SourceState state) {
    WatermarkInterval interval = getWatermarkIntervalFromMultiWorkUnit(multiWorkUnit);
    List<KafkaPartition> partitions = getPartitionsFromMultiWorkUnit(multiWorkUnit);
    Preconditions.checkArgument(!partitions.isEmpty(), "There must be at least one partition in the multiWorkUnit");
    Extract extract = this.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, partitions.get(0).getTopicName());
    WorkUnit workUnit = WorkUnit.create(extract, interval);
    populateMultiPartitionWorkUnit(partitions, workUnit);
    workUnit.setProp(ESTIMATED_DATA_SIZE, multiWorkUnit.getProp(ESTIMATED_DATA_SIZE));
    LOG.info(String.format("Created workunit for partitions %s", partitions));
    return workUnit;
  }

  @SuppressWarnings("deprecation")
  private static WatermarkInterval getWatermarkIntervalFromMultiWorkUnit(MultiWorkUnit multiWorkUnit) {
    List<Long> lowWatermarkValues = Lists.newArrayList();
    List<Long> expectedHighWatermarkValues = Lists.newArrayList();
    for (WorkUnit workUnit : multiWorkUnit.getWorkUnits()) {
      lowWatermarkValues.add(workUnit.getLowWaterMark());
      expectedHighWatermarkValues.add(workUnit.getHighWaterMark());
    }
    return new WatermarkInterval(new MultiLongWatermark(lowWatermarkValues),
        new MultiLongWatermark(expectedHighWatermarkValues));
  }

  private static List<KafkaPartition> getPartitionsFromMultiWorkUnit(MultiWorkUnit multiWorkUnit) {
    List<KafkaPartition> partitions = Lists.newArrayList();

    for (WorkUnit workUnit : multiWorkUnit.getWorkUnits()) {
      partitions.add(KafkaUtils.getPartition(workUnit));
    }

    return partitions;
  }

  /**
   * Pack a list of WorkUnits into a smaller number of MultiWorkUnits, using the worst-fit-decreasing algorithm.
   *
   * Each WorkUnit is assigned to the MultiWorkUnit with the smallest load.
   */
  private List<WorkUnit> worstFitDecreasingBinPacking(List<WorkUnit> groups, int numOfMultiWorkUnits) {

    // Sort workunit groups by data size desc
    Collections.sort(groups, SIZE_DESC_COMPARATOR);

    MinMaxPriorityQueue<MultiWorkUnit> pQueue =
        MinMaxPriorityQueue.orderedBy(SIZE_ASC_COMPARATOR).expectedSize(numOfMultiWorkUnits).create();
    for (int i = 0; i < numOfMultiWorkUnits; i++) {
      MultiWorkUnit multiWorkUnit = new MultiWorkUnit();
      setWorkUnitEstSize(multiWorkUnit, 0);
      pQueue.add(multiWorkUnit);
    }

    for (WorkUnit group : groups) {
      MultiWorkUnit lightestMultiWorkUnit = pQueue.poll();
      addWorkUnitToMultiWorkUnit(group, lightestMultiWorkUnit);
      pQueue.add(lightestMultiWorkUnit);
    }

    long minLoad = getWorkUnitEstSize(pQueue.peekFirst());
    long maxLoad = getWorkUnitEstSize(pQueue.peekLast());
    LOG.info(String.format("Min data size of multiWorkUnit = %d; Max data size of multiWorkUnit = %d; Diff = %f%%",
        minLoad, maxLoad, (double) (maxLoad - minLoad) / (double) maxLoad * 100.0));

    List<WorkUnit> multiWorkUnits = Lists.newArrayList();
    multiWorkUnits.addAll(pQueue);
    return multiWorkUnits;
  }

  /**
   * Group workUnits into groups. Each group is a MultiWorkUnit. Each group has a capacity of avgGroupSize.
   * If there's a single workUnit whose size is larger than avgGroupSize, it forms a group itself.
   */
  private List<MultiWorkUnit> bestFitDecreasingBinPacking(List<WorkUnit> workUnits, long avgGroupSize) {

    // Sort workunits by data size desc
    Collections.sort(workUnits, SIZE_DESC_COMPARATOR);

    PriorityQueue<MultiWorkUnit> pQueue = new PriorityQueue<MultiWorkUnit>(workUnits.size(), SIZE_DESC_COMPARATOR);
    for (WorkUnit workUnit : workUnits) {
      MultiWorkUnit bestGroup = findAndPopBestFitGroup(workUnit, pQueue, avgGroupSize);
      if (bestGroup != null) {
        addWorkUnitToMultiWorkUnit(workUnit, bestGroup);
      } else {
        bestGroup = new MultiWorkUnit();
        addWorkUnitToMultiWorkUnit(workUnit, bestGroup);
      }
      pQueue.add(bestGroup);
    }
    return Lists.newArrayList(pQueue);
  }

  private static void addWorkUnitToMultiWorkUnit(WorkUnit workUnit, MultiWorkUnit multiWorkUnit) {
    multiWorkUnit.addWorkUnit(workUnit);
    long size = multiWorkUnit.getPropAsLong(ESTIMATED_DATA_SIZE, 0);
    multiWorkUnit.setProp(ESTIMATED_DATA_SIZE, size + getWorkUnitEstSize(workUnit));
  }

  private static void addWorkUnitsToMultiWorkUnit(List<WorkUnit> workUnits, MultiWorkUnit multiWorkUnit) {
    for (WorkUnit workUnit : workUnits) {
      addWorkUnitToMultiWorkUnit(workUnit, multiWorkUnit);
    }
  }

  /**
   * Find the best group using the best-fit-decreasing algorithm.
   * The best group is the fullest group that has enough capacity for the new workunit.
   * If no existing group has enough capacity for the new workUnit, return null.
   */
  private MultiWorkUnit findAndPopBestFitGroup(WorkUnit workUnit, PriorityQueue<MultiWorkUnit> pQueue,
      long avgGroupSize) {

    List<MultiWorkUnit> fullWorkUnits = Lists.newArrayList();
    MultiWorkUnit bestFit = null;

    while (!pQueue.isEmpty()) {
      MultiWorkUnit candidate = pQueue.poll();
      if (getWorkUnitEstSize(candidate) + getWorkUnitEstSize(workUnit) <= avgGroupSize) {
        bestFit = candidate;
        break;
      } else {
        fullWorkUnits.add(candidate);
      }
    }

    for (MultiWorkUnit fullWorkUnit : fullWorkUnits) {
      pQueue.add(fullWorkUnit);
    }

    return bestFit;
  }

  private static long calcTotalEstSizeForTopic(List<WorkUnit> workUnitsForTopic) {
    long totalSize = 0;
    for (WorkUnit w : workUnitsForTopic) {
      totalSize += getWorkUnitEstSize(w);
    }
    return totalSize;
  }

  private long getPreviousAvgSizeForPartition(KafkaPartition partition) {
    if (this.previousAvgSizes.containsKey(partition)) {
      LOG.info(String.format("Estimated avg event size for partition %s is %d", partition,
          this.previousAvgSizes.get(partition)));
      return this.previousAvgSizes.get(partition);
    } else {
      LOG.warn(String.format("Avg event size for partition %s not available, using default size %d", partition,
          DEFAULT_AVG_EVENT_SIZE));
      return DEFAULT_AVG_EVENT_SIZE;
    }
  }

  private void getAllPreviousAvgSizes(SourceState state) {
    this.previousAvgSizes.clear();
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      for (KafkaPartition partition : partitions) {
        long previousAvgSize = KafkaUtils.getPartitionAvgEventSize(workUnitState, partition, DEFAULT_AVG_EVENT_SIZE);
        this.previousAvgSizes.put(partition, previousAvgSize);
      }
    }
  }

  private List<WorkUnit> getWorkUnitsForTopic(KafkaWrapper kafkaWrapper, KafkaTopic topic, SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (KafkaPartition partition : topic.getPartitions()) {
      WorkUnit workUnit = getWorkUnitForTopicPartition(kafkaWrapper, partition, state);
      this.partitionsToBeProcessed.add(partition);
      if (workUnit != null) {
        workUnits.add(workUnit);
      }
    }
    return workUnits;
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaWrapper kafkaWrapper, KafkaPartition partition,
      SourceState state) {
    Offsets offsets = new Offsets();

    boolean failedToGetKafkaOffsets = false;

    try {
      offsets.setEarliestOffset(kafkaWrapper.getEarliestOffset(partition));
      offsets.setLatestOffset(kafkaWrapper.getLatestOffset(partition));
    } catch (KafkaOffsetRetrievalFailureException e) {
      failedToGetKafkaOffsets = true;
    }

    long previousOffset = 0;
    boolean previousOffsetNotFound = false;
    try {
      previousOffset = getPreviousOffsetForPartition(partition, state);
    } catch (PreviousOffsetNotFoundException e) {
      previousOffsetNotFound = true;
    }

    if (failedToGetKafkaOffsets) {

      // Increment counts, which will be reported as job metrics
      this.failToGetOffsetCount++;

      // When unable to get earliest/latest offsets from Kafka, skip the partition and create an empty workunit,
      // so that previousOffset is persisted.
      LOG.warn(String.format(
          "Failed to retrieve earliest and/or latest offset for partition %s. This partition will be skipped.",
          partition));
      return previousOffsetNotFound ? null : createEmptyWorkUnit(partition, previousOffset);
    }

    if (shouldMoveToLatestOffset(partition, state)) {
      offsets.startAtLatestOffset();
    } else if (previousOffsetNotFound) {

      // When previous offset cannot be found, either start at earliest offset or latest offset, or skip the partition
      // (no need to create an empty workunit in this case since there's no offset to persist).
      String offsetNotFoundMsg = String.format("Previous offset for partition %s does not exist. ", partition);
      String offsetOption = state.getProp(BOOTSTRAP_WITH_OFFSET, DEFAULT_BOOTSTRAP_WITH_OFFSET).toLowerCase();
      if (offsetOption.equals(LATEST_OFFSET)) {
        LOG.warn(offsetNotFoundMsg + "This partition will start from the latest offset: " + offsets.getLatestOffset());
        offsets.startAtLatestOffset();
      } else if (offsetOption.equals(EARLIEST_OFFSET)) {
        LOG.warn(
            offsetNotFoundMsg + "This partition will start from the earliest offset: " + offsets.getEarliestOffset());
        offsets.startAtEarliestOffset();
      } else {
        LOG.warn(offsetNotFoundMsg + "This partition will be skipped.");
        return null;
      }
    } else {
      try {
        offsets.startAt(previousOffset);
      } catch (StartOffsetOutOfRangeException e) {

        // Increment counts, which will be reported as job metrics
        if (offsets.getStartOffset() <= offsets.getLatestOffset()) {
          this.offsetTooEarlyCount++;
        } else {
          this.offsetTooLateCount++;
        }

        // When previous offset is out of range, either start at earliest, latest or nearest offset, or skip the
        // partition. If skipping, need to create an empty workunit so that previousOffset is persisted.
        String offsetOutOfRangeMsg = String.format(String.format(
            "Start offset for partition %s is out of range. Start offset = %d, earliest offset = %d, latest offset = %d.",
            partition, offsets.getStartOffset(), offsets.getEarliestOffset(), offsets.getLatestOffset()));
        String offsetOption =
            state.getProp(RESET_ON_OFFSET_OUT_OF_RANGE, DEFAULT_RESET_ON_OFFSET_OUT_OF_RANGE).toLowerCase();
        if (offsetOption.equals(LATEST_OFFSET)
            || (offsetOption.equals(NEAREST_OFFSET) && offsets.getStartOffset() >= offsets.getLatestOffset())) {
          LOG.warn(
              offsetOutOfRangeMsg + "This partition will start from the latest offset: " + offsets.getLatestOffset());
          offsets.startAtLatestOffset();
        } else if (offsetOption.equals(EARLIEST_OFFSET) || offsetOption.equals(NEAREST_OFFSET)) {
          LOG.warn(offsetOutOfRangeMsg + "This partition will start from the earliest offset: "
              + offsets.getEarliestOffset());
          offsets.startAtEarliestOffset();
        } else {
          LOG.warn(offsetOutOfRangeMsg + "This partition will be skipped.");
          return createEmptyWorkUnit(partition, previousOffset);
        }
      }
    }

    return getWorkUnitForTopicPartition(partition, offsets);
  }

  private long getPreviousOffsetForPartition(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {
    if (this.previousOffsets.isEmpty()) {
      getAllPreviousOffsets(state);
    }
    if (this.previousOffsets.containsKey(partition)) {
      return this.previousOffsets.get(partition);
    }
    throw new PreviousOffsetNotFoundException(String.format("Previous offset for topic %s, partition %s not found.",
        partition.getTopicName(), partition.getId()));
  }

  private void getAllPreviousOffsets(SourceState state) {
    this.previousOffsets.clear();
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      MultiLongWatermark watermark = getWatermark(workUnitState);
      Preconditions.checkArgument(partitions.size() == watermark.size(), String.format(
          "Num of partitions doesn't match number of watermarks: partitions=%s, watermarks=%s", partitions, watermark));
      for (int i = 0; i < partitions.size(); i++) {
        if (watermark.get(i) != ConfigurationKeys.DEFAULT_WATERMARK_VALUE)
          this.previousOffsets.put(partitions.get(i), watermark.get(i));
      }
    }
  }

  private MultiLongWatermark getWatermark(WorkUnitState workUnitState) {
    if (workUnitState.getActualHighWatermark() != null) {
      return GSON.fromJson(workUnitState.getActualHighWatermark(), MultiLongWatermark.class);
    } else if (workUnitState.getWorkunit().getLowWatermark() != null) {
      return GSON.fromJson(workUnitState.getWorkunit().getLowWatermark(), MultiLongWatermark.class);
    }
    throw new IllegalArgumentException(
        String.format("workUnitState %s doesn't have either actual high watermark or low watermark", workUnitState));
  }

  /**
   * A topic can be configured to move to the latest offset in topics.move.to.latest.offset
   */
  private boolean shouldMoveToLatestOffset(KafkaPartition partition, SourceState state) {
    if (!state.contains(TOPICS_MOVE_TO_LATEST_OFFSET)) {
      return false;
    }
    if (this.moveToLatestTopics.isEmpty()) {
      this.moveToLatestTopics.addAll(
          Splitter.on(',').trimResults().omitEmptyStrings().splitToList(state.getProp(TOPICS_MOVE_TO_LATEST_OFFSET)));
    }
    return this.moveToLatestTopics.contains(partition.getTopicName()) || moveToLatestTopics.contains(ALL_TOPICS);
  }

  private WorkUnit createEmptyWorkUnit(KafkaPartition partition, long previousOffset) {
    Offsets offsets = new Offsets();
    offsets.setEarliestOffset(previousOffset);
    offsets.setLatestOffset(previousOffset);
    offsets.startAtEarliestOffset();
    return getWorkUnitForTopicPartition(partition, offsets);
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, Offsets offsets) {
    Extract extract = this.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, partition.getTopicName());
    WorkUnit workUnit = WorkUnit.create(extract);
    workUnit.setProp(TOPIC_NAME, partition.getTopicName());
    workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partition.getTopicName());
    workUnit.setProp(PARTITION_ID, partition.getId());
    workUnit.setProp(LEADER_ID, partition.getLeader().getId());
    workUnit.setProp(LEADER_HOSTANDPORT, partition.getLeader().getHostAndPort().toString());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, offsets.getStartOffset());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, offsets.getLatestOffset());
    LOG.info(String.format("Created workunit for partition %s: lowWatermark=%d, highWatermark=%d", partition,
        offsets.getStartOffset(), offsets.getLatestOffset()));
    return workUnit;
  }

  /**
   * Add a list of partitions of the same topic to a workUnit.
   */
  private static void populateMultiPartitionWorkUnit(List<KafkaPartition> partitions, WorkUnit workUnit) {
    Preconditions.checkArgument(!partitions.isEmpty(), "There should be at least one partition");
    workUnit.setProp(TOPIC_NAME, partitions.get(0).getTopicName());
    GobblinMetrics.addCustomTagToState(workUnit, new Tag<String>("kafkaTopic", partitions.get(0).getTopicName()));
    workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partitions.get(0).getTopicName());
    for (int i = 0; i < partitions.size(); i++) {
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i), partitions.get(i).getId());
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_ID, i),
          partitions.get(i).getLeader().getId());
      workUnit.setProp(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_HOSTANDPORT, i),
          partitions.get(i).getLeader().getHostAndPort());
    }
  }

  private List<KafkaTopic> getFilteredTopics(KafkaWrapper kafkaWrapper, SourceState state) {
    List<Pattern> blacklist = getBlacklist(state);
    List<Pattern> whitelist = getWhitelist(state);
    return kafkaWrapper.getFilteredTopics(blacklist, whitelist);
  }

  private static List<Pattern> getBlacklist(State state) {
    List<String> list = state.getPropAsList(TOPIC_BLACKLIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private static List<Pattern> getWhitelist(State state) {
    List<String> list = state.getPropAsList(TOPIC_WHITELIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  @Override
  public void shutdown(SourceState state) {
    state.setProp(ConfigurationKeys.OFFSET_TOO_EARLY_COUNT, this.offsetTooEarlyCount);
    state.setProp(ConfigurationKeys.OFFSET_TOO_LATE_COUNT, this.offsetTooLateCount);
    state.setProp(ConfigurationKeys.FAIL_TO_GET_OFFSET_COUNT, this.failToGetOffsetCount);
  }

  /**
   * This class contains startOffset, earliestOffset and latestOffset for a Kafka partition.
   */
  private static class Offsets {
    private long startOffset = 0;
    private long earliestOffset = 0;
    private long latestOffset = 0;

    private void setEarliestOffset(long offset) {
      this.earliestOffset = offset;
    }

    private long getEarliestOffset() {
      return this.earliestOffset;
    }

    private void setLatestOffset(long offset) {
      this.latestOffset = offset;
    }

    private long getLatestOffset() {
      return this.latestOffset;
    }

    private void startAt(long offset) throws StartOffsetOutOfRangeException {
      if (offset < this.earliestOffset || offset > this.latestOffset + 1) {
        throw new StartOffsetOutOfRangeException(
            String.format("start offset = %d, earliest offset = %d, latest offset = %d", offset, this.earliestOffset,
                this.latestOffset));
      }
      this.startOffset = offset;
    }

    private void startAtEarliestOffset() {
      this.startOffset = this.earliestOffset;
    }

    private void startAtLatestOffset() {
      this.startOffset = this.latestOffset;
    }

    private long getStartOffset() {
      return this.startOffset;
    }
  }

}
