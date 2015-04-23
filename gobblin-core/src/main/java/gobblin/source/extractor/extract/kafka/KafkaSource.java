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
  public static final String NUM_OF_MAPPERS = "num.of.mappers";
  public static final int DEFAULT_NUM_OF_MAPPERS = 100;
  public static final String AVG_EVENT_SIZE = "avg.event.size";
  public static final long DEFAULT_AVG_EVENT_SIZE = 1024;

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newHashMap();
  private final Map<KafkaPartition, Long> previousAvgSizes = Maps.newHashMap();

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
      int numOfMultiWorkunits = state.getPropAsInt(NUM_OF_MAPPERS, DEFAULT_NUM_OF_MAPPERS);
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
   * while preferring to put partitions of the same topic into the same multiWorkUnits (in order to
   * avoid generating many small mapper output files).
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

    List<List<WorkUnitWithEstSize>> workUnitsWithEstSize = getWorkUnitsWithEstSize(workUnits);
    long totalEstimatedDataSize = calcTotalEstSize(workUnitsWithEstSize);
    long avgGroupSize = (long) ((double) totalEstimatedDataSize / (double) numOfMultiWorkunits / 3.0);

    List<WorkUnitGroup> groups = Lists.newArrayList();
    for (List<WorkUnitWithEstSize> workUnitsWithEstSizeForTopic : workUnitsWithEstSize) {
      long estimatedDataSizeForTopic = calcTotalEstSizeForTopic(workUnitsWithEstSizeForTopic);
      if (estimatedDataSizeForTopic < avgGroupSize) {

        // If the total estimated size of a topic is smaller than group size, put all partitions of this
        // topic in a single group.
        groups.add(new WorkUnitGroup(workUnitsWithEstSizeForTopic));
      } else {

        // Use best-fit-decreasing to group workunits for a topic into multiple groups.
        groups.addAll(bestFitDecreasingBinPacking(workUnitsWithEstSizeForTopic, avgGroupSize));
      }
    }

    return worstFitDecreasingBinPacking(groups, numOfMultiWorkunits);
  }

  /**
   * Pack a list of WorkUnitGroups into multiWorkUnits, using the worst-fit-decreasing algorithm,
   * i.e., for each group, assign it to the multiWorkUnit with the lightest load.
   */
  private static List<WorkUnit> worstFitDecreasingBinPacking(List<WorkUnitGroup> groups, int numOfMultiWorkUnits) {
    List<MultiWorkUnit> multiWorkunits = Lists.newArrayListWithCapacity(numOfMultiWorkUnits);
    List<Long> loads = Lists.newArrayListWithCapacity(numOfMultiWorkUnits);
    for (int i = 0; i < numOfMultiWorkUnits; i++) {
      multiWorkunits.add(new MultiWorkUnit());
      loads.add(0L);
    }

    // Sort workunit groups by data size desc
    Collections.sort(groups, sortBySizeDescComparator());

    for (WorkUnitGroup group : groups) {
      int worstMultiWorkUnitId = findWorstFitMultiWorkUnitId(multiWorkunits, loads);
      multiWorkunits.get(worstMultiWorkUnitId).addWorkUnits(group.getWorkUnits());
      loads.set(worstMultiWorkUnitId, loads.get(worstMultiWorkUnitId) + group.getEstDataSize());
    }

    long minLoad = Collections.min(loads);
    long maxLoad = Collections.max(loads);
    LOG.info(String.format("Min data size of multiWorkUnit = %d; Max data size of multiWorkUnit = %d; Diff = %f%%",
        minLoad, maxLoad, (double) (maxLoad - minLoad) / (double) maxLoad * 100.0));

    return Lists.newArrayList(multiWorkunits);
  }

  /**
   * Find the worst-fit multiWorkUnit, i.e., the multiWorkUnit that currently has the lowest load.
   * @param loads 
   */
  private static int findWorstFitMultiWorkUnitId(List<MultiWorkUnit> multiWorkunits, List<Long> loads) {
    Preconditions.checkArgument(multiWorkunits.size() == loads.size());

    long smallestLoad = Long.MAX_VALUE;
    int smallestId = -1;
    for (int i = 0; i < loads.size(); i++) {
      if (smallestLoad > loads.get(i)) {
        smallestLoad = loads.get(i);
        smallestId = i;
      }
    }

    return smallestId;
  }

  /**
   * Group workUnits into groups. Each group has a capacity of avgGroupSize. If there's a single
   * workUnit whose size is larger than avgGroupSize, it forms a group itself.
   */
  private static List<WorkUnitGroup> bestFitDecreasingBinPacking(List<WorkUnitWithEstSize> workUnitsWithEstSize,
      long avgGroupSize) {

    // Sort workunits by data size desc
    Collections.sort(workUnitsWithEstSize, sortBySizeDescComparator());

    List<WorkUnitGroup> groups = Lists.newArrayList();
    for (WorkUnitWithEstSize workUnitWithEstSize : workUnitsWithEstSize) {
      WorkUnitGroup bestGroup = findBestFitGroup(workUnitWithEstSize, groups, avgGroupSize);
      if (bestGroup != null) {
        bestGroup.addWorkUnit(workUnitWithEstSize);
      } else {
        bestGroup = new WorkUnitGroup();
        bestGroup.addWorkUnit(workUnitWithEstSize);
        groups.add(bestGroup);
      }
    }
    return groups;
  }

  /**
   * Find the best group using the best-fit-decreasing algorithm.
   * If no existing group has enough capacity for the new workUnit, return null.
   * @param avgGroupSize 
   */
  private static WorkUnitGroup findBestFitGroup(WorkUnitWithEstSize workUnitWithEstSize, List<WorkUnitGroup> groups,
      long avgGroupSize) {
    WorkUnitGroup bestGroup = null;
    long highestLoad = Long.MIN_VALUE;

    for (WorkUnitGroup candidateGroup : groups) {
      if (candidateGroup.hasEnoughCapacity(avgGroupSize, workUnitWithEstSize.getEstDataSize())
          && highestLoad < candidateGroup.getEstDataSize()) {
        highestLoad = candidateGroup.getEstDataSize();
        bestGroup = candidateGroup;
      }
    }
    return bestGroup;
  }

  /**
   * A comparator for sorting workunits or workunit groups in descending order of estimated data size.
   */
  private static Comparator<WorkUnitWithEstSize> sortBySizeDescComparator() {
    return new Comparator<WorkUnitWithEstSize>() {
      @Override
      public int compare(WorkUnitWithEstSize w1, WorkUnitWithEstSize w2) {
        return ((Long) w2.getEstDataSize()).compareTo(w1.getEstDataSize());
      }
    };
  }

  private static long calcTotalEstSizeForTopic(List<WorkUnitWithEstSize> workUnitsWithEstSizeForTopic) {
    long totalSize = 0;
    for (WorkUnitWithEstSize w : workUnitsWithEstSizeForTopic) {
      totalSize += w.getEstDataSize();
    }
    return totalSize;
  }

  private static long calcTotalEstSize(List<List<WorkUnitWithEstSize>> workUnitsWithEstSize) {
    long totalSize = 0;
    for (List<WorkUnitWithEstSize> list : workUnitsWithEstSize) {
      totalSize += calcTotalEstSizeForTopic(list);
    }
    return totalSize;
  }

  private List<List<WorkUnitWithEstSize>> getWorkUnitsWithEstSize(List<List<WorkUnit>> workUnits) {
    List<List<WorkUnitWithEstSize>> workUnitsWithEstSize = Lists.newArrayListWithCapacity(workUnits.size());
    for (List<WorkUnit> workUnitsForTopic : workUnits) {
      List<WorkUnitWithEstSize> workUnitsWithEstSizeForTopic = Lists.newArrayListWithCapacity(workUnitsForTopic.size());
      for (WorkUnit workUnit : workUnitsForTopic) {
        long avgSize = this.getPreviousAvgSizeForPartition(this.getKafkaPartitionFromState(workUnit));
        long numOfEvents =
            workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
                - workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
        workUnitsWithEstSizeForTopic.add(new WorkUnitWithEstSize(workUnit, avgSize * numOfEvents));
      }
      workUnitsWithEstSize.add(workUnitsWithEstSizeForTopic);
    }
    return workUnitsWithEstSize;
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

  private static class WorkUnitGroup extends WorkUnitWithEstSize {
    private final List<WorkUnit> workUnits;

    private WorkUnitGroup() {
      this.workUnits = Lists.newArrayList();
      this.estDataSize = 0;
    }

    private WorkUnitGroup(List<WorkUnitWithEstSize> workUnitsWithEstSizeForTopic) {
      this();
      for (WorkUnitWithEstSize workUnitWithEstSize : workUnitsWithEstSizeForTopic) {
        this.addWorkUnit(workUnitWithEstSize);
      }
    }

    private boolean hasEnoughCapacity(long totalCapacity, long request) {
      return this.estDataSize + request <= totalCapacity;
    }

    private List<WorkUnit> getWorkUnits() {
      return workUnits;
    }

    private void addWorkUnit(WorkUnitWithEstSize workUnitWithEstSize) {
      this.workUnits.add(workUnitWithEstSize.getWorkUnit());
      this.estDataSize += workUnitWithEstSize.getEstDataSize();
    }
  }

  private static class WorkUnitWithEstSize {
    private WorkUnit workUnit;
    protected long estDataSize;

    private WorkUnitWithEstSize() {
      this.workUnit = null;
      this.estDataSize = 0;
    }

    private WorkUnitWithEstSize(WorkUnit workUnit, long estDataSize) {
      this.workUnit = workUnit;
      this.estDataSize = estDataSize;
    }

    private WorkUnit getWorkUnit() {
      return workUnit;
    }

    protected long getEstDataSize() {
      return estDataSize;
    }
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
