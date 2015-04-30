/* (c) 2015 LinkedIn Corp. All rights reserved.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.EventBasedSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Longs;


/**
 * A {@link gobblin.source.Source} implementation for Kafka source.
 *
 * @author ziliu
 */
public abstract class KafkaSource extends EventBasedSource<Schema, GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  public static final String TOPIC_BLACKLIST = "topic.blacklist";
  public static final String TOPIC_WHITELIST = "topic.whitelist";
  public static final String TOPICS_MOVE_TO_LATEST_OFFSET = "topics.move.to.latest.offset";
  public static final String MOVE_TO_EARLIEST_OFFSET_ALLOWED = "move.to.earliest.offset.allowed";
  public static final boolean DEFAULT_MOVE_TO_EARLIEST_OFFSET_ALLOWED = false;
  public static final String TOPIC_NAME = "topic.name";
  public static final String PARTITION_ID = "partition.id";
  public static final String LEADER_ID = "leader.id";
  public static final String LEADER_HOST = "leader.host";
  public static final String LEADER_PORT = "leader.port";
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "KAFKA";
  public static final String ALL_TOPICS = "all";
  public static final String AVG_EVENT_SIZE = "avg.event.size";
  public static final long DEFAULT_AVG_EVENT_SIZE = 1024;
  public static final String ESTIMATED_DATA_SIZE = "estimated.data.size";

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newHashMap();
  private final Map<KafkaPartition, Long> previousAvgSizes = Maps.newHashMap();

  private final Comparator<WorkUnit> sortBySizeAscComparator = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Longs.compare(getWorkUnitEstSize(w1), getWorkUnitEstSize(w2));
    }
  };

  private final Comparator<WorkUnit> sortBySizeDescComparator = new Comparator<WorkUnit>() {
    @Override
    public int compare(WorkUnit w1, WorkUnit w2) {
      return Longs.compare(getWorkUnitEstSize(w2), getWorkUnitEstSize(w1));
    }
  };

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<List<WorkUnit>> workUnits = Lists.newArrayList();
    Closer closer = Closer.create();
    try {
      KafkaWrapper kafkaWrapper = closer.register(KafkaWrapper.create(state));
      List<KafkaTopic> topics = getFilteredTopics(kafkaWrapper, state);
      for (KafkaTopic topic : topics) {
        workUnits.add(getWorkUnitsForTopic(kafkaWrapper, topic, state));
      }
      int numOfMultiWorkunits =
          state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);
      this.getAllPreviousAvgSizes(state);
      return ImmutableList.copyOf(getMultiWorkunits(workUnits, numOfMultiWorkunits));
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to close kafkaWrapper", e);
      }
    }
  }

  /**
   * Group workUnits into multiWorkUnits. Each workUnit corresponds to a (topic, partition).
   * The goal is to group the workUnits as evenly as possible into numOfMultiWorkunits multiWorkunits,
   * while preferring to put partitions of the same topic into the same multiWorkUnits.
   *
   * The algorithm is to first group workUnits into approximately 3 * numOfMultiWorkunits groups using
   * best-fit-decreasing, such that partitions of the same topic are in the same group. Then, 
   * assemble these groups into multiWorkunits using worst-fit-decreasing.
   *
   * The reason behind 3 is that the worst-fit-decreasing algorithm should work well if the number of
   * items is more than 3 times the number of bins.
   *
   * The input workUnits are partitioned by topic.
   */
  private List<WorkUnit> getMultiWorkunits(List<List<WorkUnit>> workUnits, int numOfMultiWorkunits) {
    Preconditions.checkArgument(numOfMultiWorkunits >= 1);

    long totalEstDataSize = 0;
    for (List<WorkUnit> workUnitsForTopic : workUnits) {
      for (WorkUnit workUnit : workUnitsForTopic) {
        setWorkUnitEstSize(workUnit);
        totalEstDataSize += getWorkUnitEstSize(workUnit);
      }
    }
    long avgGroupSize = (long) ((double) totalEstDataSize / (double) numOfMultiWorkunits / 3.0);

    List<MultiWorkUnit> groups = Lists.newArrayList();
    for (List<WorkUnit> workUnitsForTopic : workUnits) {
      long estimatedDataSizeForTopic = calcTotalEstSizeForTopic(workUnitsForTopic);
      if (estimatedDataSizeForTopic < avgGroupSize) {

        // If the total estimated size of a topic is smaller than group size, put all partitions of this
        // topic in a single group.
        MultiWorkUnit group = new MultiWorkUnit();
        this.addWorkUnitsToMultiWorkUnit(workUnitsForTopic, group);
        groups.add(group);
      } else {

        // Use best-fit-decreasing to group workunits for a topic into multiple groups.
        groups.addAll(bestFitDecreasingBinPacking(workUnitsForTopic, avgGroupSize));
      }
    }

    return worstFitDecreasingBinPacking(groups, numOfMultiWorkunits);
  }

  private void setWorkUnitEstSize(WorkUnit workUnit) {
    long avgSize = this.getPreviousAvgSizeForPartition(this.getKafkaPartitionFromState(workUnit));
    long numOfEvents =
        workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
            - workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    workUnit.setProp(ESTIMATED_DATA_SIZE, avgSize * numOfEvents);
  }

  private void setWorkUnitEstSize(WorkUnit workUnit, long estSize) {
    workUnit.setProp(ESTIMATED_DATA_SIZE, estSize);
  }

  private long getWorkUnitEstSize(WorkUnit workUnit) {
    Preconditions.checkArgument(workUnit.contains(ESTIMATED_DATA_SIZE));
    return workUnit.getPropAsLong(ESTIMATED_DATA_SIZE);
  }

  /**
   * Pack a list of multiWorkUnits into a smaller number of multiWorkUnits, using the worst-fit-decreasing algorithm.
   */
  private List<WorkUnit> worstFitDecreasingBinPacking(List<MultiWorkUnit> groups, int numOfMultiWorkUnits) {

    // Sort workunit groups by data size desc
    Collections.sort(groups, this.sortBySizeDescComparator);

    Queue<MultiWorkUnit> pQueue = new PriorityQueue<MultiWorkUnit>(numOfMultiWorkUnits, this.sortBySizeAscComparator);

    for (int i = 0; i < numOfMultiWorkUnits; i++) {
      MultiWorkUnit multiWorkUnit = new MultiWorkUnit();
      this.setWorkUnitEstSize(multiWorkUnit, 0);
      pQueue.add(multiWorkUnit);
    }

    for (MultiWorkUnit group : groups) {
      MultiWorkUnit lightestMultiWorkUnit = pQueue.poll();
      this.addWorkUnitsToMultiWorkUnit(group.getWorkUnits(), lightestMultiWorkUnit);
      pQueue.add(lightestMultiWorkUnit);
    }

    List<WorkUnit> multiWorkUnits = Lists.newArrayList();
    multiWorkUnits.addAll(pQueue);
    Collections.sort(multiWorkUnits, this.sortBySizeAscComparator);
    long minLoad = this.getWorkUnitEstSize(multiWorkUnits.get(0));
    long maxLoad = this.getWorkUnitEstSize(multiWorkUnits.get(multiWorkUnits.size() - 1));
    LOG.info(String.format("Min data size of multiWorkUnit = %d; Max data size of multiWorkUnit = %d; Diff = %f%%",
        minLoad, maxLoad, (double) (maxLoad - minLoad) / (double) maxLoad * 100.0));

    return multiWorkUnits;
  }

  /**
   * Group workUnits into groups. Each group has a capacity of avgGroupSize. If there's a single
   * workUnit whose size is larger than avgGroupSize, it forms a group itself.
   */
  private List<MultiWorkUnit> bestFitDecreasingBinPacking(List<WorkUnit> workUnits, long avgGroupSize) {

    // Sort workunits by data size desc
    Collections.sort(workUnits, this.sortBySizeDescComparator);

    Queue<MultiWorkUnit> pQueue = new PriorityQueue<MultiWorkUnit>(workUnits.size(), this.sortBySizeDescComparator);
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

  private void addWorkUnitToMultiWorkUnit(WorkUnit workUnit, MultiWorkUnit multiWorkUnit) {
    multiWorkUnit.addWorkUnit(workUnit);
    long size = multiWorkUnit.getPropAsLong(ESTIMATED_DATA_SIZE, 0);
    multiWorkUnit.setProp(ESTIMATED_DATA_SIZE, size + getWorkUnitEstSize(workUnit));
  }

  private void addWorkUnitsToMultiWorkUnit(List<WorkUnit> workUnits, MultiWorkUnit multiWorkUnit) {
    for (WorkUnit workUnit : workUnits) {
      this.addWorkUnitToMultiWorkUnit(workUnit, multiWorkUnit);
    }

  }

  /**
   * Find the best group using the best-fit-decreasing algorithm.
   * If no existing group has enough capacity for the new workUnit, return null.
   * @param avgGroupSize 
   */
  private MultiWorkUnit findAndPopBestFitGroup(WorkUnit workUnit, Queue<MultiWorkUnit> pQueue, long avgGroupSize) {
    if (pQueue.isEmpty()) {
      return null;
    }

    long currGroupSize = getWorkUnitEstSize(pQueue.peek());

    if (currGroupSize + getWorkUnitEstSize(workUnit) <= avgGroupSize) {
      return pQueue.poll();
    } else {
      return null;
    }
  }

  private long calcTotalEstSizeForTopic(List<WorkUnit> workUnitsForTopic) {
    long totalSize = 0;
    for (WorkUnit w : workUnitsForTopic) {
      totalSize += this.getWorkUnitEstSize(w);
    }
    return totalSize;
  }

  private long getPreviousAvgSizeForPartition(KafkaPartition partition) {
    if (this.previousAvgSizes.containsKey(partition)) {
      LOG.info(String.format("Estimated avg event size for partition %s:%d is %d", partition.getTopicName(),
          partition.getId(), this.previousAvgSizes.get(partition)));
      return this.previousAvgSizes.get(partition);
    } else {
      LOG.warn(String.format("Avg event size for partition %s:%d not available, using default size %d",
          partition.getTopicName(), partition.getId(), DEFAULT_AVG_EVENT_SIZE));
      return DEFAULT_AVG_EVENT_SIZE;
    }
  }

  private void getAllPreviousAvgSizes(SourceState state) {
    this.previousAvgSizes.clear();
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      KafkaPartition partition = getKafkaPartitionFromState(workUnitState);
      if (partition == null) {
        continue;
      }
      long previousAvgSize =
          workUnitState.getPropAsLong(getWorkUnitSizePropName(workUnitState), DEFAULT_AVG_EVENT_SIZE);
      this.previousAvgSizes.put(partition, previousAvgSize);
    }
  }

  static String getWorkUnitSizePropName(State state) {
    return state.getProp(TOPIC_NAME) + "." + state.getPropAsInt(PARTITION_ID) + "." + AVG_EVENT_SIZE;
  }

  private List<WorkUnit> getWorkUnitsForTopic(KafkaWrapper kafkaWrapper, KafkaTopic topic, SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (KafkaPartition partition : topic.getPartitions()) {
      WorkUnit workUnit = getWorkUnitForTopicPartition(kafkaWrapper, partition, state);
      if (workUnit != null) {
        workUnits.add(workUnit);
      }
    }
    return workUnits;
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaWrapper kafkaWrapper, KafkaPartition partition, SourceState state) {
    Offsets offsets = new Offsets();

    try {
      offsets.setEarliestOffset(kafkaWrapper.getEarliestOffset(partition));
      offsets.setLatestOffset(kafkaWrapper.getLatestOffset(partition));
      if (shouldMoveToLatestOffset(partition, state)) {
        offsets.startAtLatestOffset();
      } else {
        offsets.startAt(getPreviousOffsetForPartition(partition, state));
      }
    } catch (KafkaOffsetRetrievalFailureException e) {
      LOG.warn(String
          .format(
              "Failed to retrieve earliest and/or latest offset for topic %s, partition %s. This partition will be skipped.",
              partition.getTopicName(), partition.getId()));
      return null;

    } catch (PreviousOffsetNotFoundException e) {
      LOG.warn(String
          .format(
              "Previous offset for topic %s, partition %s does not exist. This partition will start from the earliest offset: %d",
              partition.getTopicName(), partition.getId(), offsets.getEarliestOffset()));
      offsets.startAtEarliestOffset();

    } catch (StartOffsetOutOfRangeException e) {
      LOG.warn(String
          .format(
              "Start offset for topic %s, partition %s is out of range. Start offset = %d, earliest offset = %d, latest offset = %d.",
              partition.getTopicName(), partition.getId(), offsets.getStartOffset(), offsets.getEarliestOffset(),
              offsets.getLatestOffset()));
      if (movingToEarliestOffsetAllowed(state)) {
        LOG.warn("Moving to earliest offset allowed. This partition will start from the earliest offset: %d",
            offsets.getEarliestOffset());
        offsets.startAtEarliestOffset();
      } else {
        LOG.warn("Moving to earliest offset not allowed. This partition will be skipped.");
        return null;
      }
    }

    return getWorkUnitForTopicPartition(partition, state, offsets);
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
      KafkaPartition partition = getKafkaPartitionFromState(workUnitState);
      if (partition == null) {
        continue;
      }
      long previousOffset = workUnitState.getHighWaterMark();
      if (previousOffset != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
        this.previousOffsets.put(partition, previousOffset);
      }
    }
  }

  private KafkaPartition getKafkaPartitionFromState(State state) {
    Preconditions.checkArgument(state.contains(TOPIC_NAME) && state.contains(PARTITION_ID));

    return new KafkaPartition.Builder().withTopicName(state.getProp(TOPIC_NAME))
        .withId(state.getPropAsInt(PARTITION_ID)).build();
  }

  /**
   * A topic can be configured to move to the latest offset in topics.move.to.latest.offset
   */
  private boolean shouldMoveToLatestOffset(KafkaPartition partition, SourceState state) {
    if (!state.contains(TOPICS_MOVE_TO_LATEST_OFFSET)) {
      return false;
    }
    if (this.moveToLatestTopics.isEmpty()) {
      this.moveToLatestTopics.addAll(Splitter.on(',').trimResults().omitEmptyStrings()
          .splitToList(state.getProp(TOPICS_MOVE_TO_LATEST_OFFSET)));
    }
    return this.moveToLatestTopics.contains(partition.getTopicName()) || moveToLatestTopics.contains(ALL_TOPICS);
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, SourceState state, Offsets offsets) {
    SourceState partitionState = createAndPopulatePartitionState(partition, state, offsets);
    Extract extract = createExtract(partitionState, partition.getTopicName());
    LOG.info(String.format("Creating workunit for topic %s, partition %d: lowWatermark=%d, highWatermark=%d",
        partition.getTopicName(), partition.getId(), offsets.getStartOffset(), offsets.getLatestOffset()));
    return partitionState.createWorkUnit(extract);
  }

  private Extract createExtract(SourceState partitionState, String topicName) {
    return partitionState.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, topicName);
  }

  private SourceState createAndPopulatePartitionState(KafkaPartition partition, SourceState state, Offsets offsets) {
    SourceState partitionState = new SourceState();
    partitionState.addAll(state);
    partitionState.setProp(TOPIC_NAME, partition.getTopicName());
    partitionState.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partition.getTopicName());
    partitionState.setProp(PARTITION_ID, partition.getId());
    partitionState.setProp(LEADER_ID, partition.getLeader().getId());
    partitionState.setProp(LEADER_HOST, partition.getLeader().getHost());
    partitionState.setProp(LEADER_PORT, partition.getLeader().getPort());
    partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, offsets.getStartOffset());
    partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, offsets.getLatestOffset());
    return partitionState;
  }

  private boolean movingToEarliestOffsetAllowed(SourceState state) {
    return state.getPropAsBoolean(MOVE_TO_EARLIEST_OFFSET_ALLOWED, DEFAULT_MOVE_TO_EARLIEST_OFFSET_ALLOWED);
  }

  private List<KafkaTopic> getFilteredTopics(KafkaWrapper kafkaWrapper, SourceState state) {
    Set<String> blacklist =
        state.contains(TOPIC_BLACKLIST) ? state.getPropAsCaseInsensitiveSet(TOPIC_BLACKLIST) : new HashSet<String>();
    Set<String> whitelist =
        state.contains(TOPIC_WHITELIST) ? state.getPropAsCaseInsensitiveSet(TOPIC_WHITELIST) : new HashSet<String>();
    return kafkaWrapper.getFilteredTopics(blacklist, whitelist);
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
        throw new StartOffsetOutOfRangeException(String.format(
            "start offset = %d, earliest offset = %d, latest offset = %d", offset, this.earliestOffset,
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
