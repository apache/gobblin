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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.MessageBasedSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract.TableType;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;


/**
 * Kafka source implementation
 *
 * @author ziliu
 *
 */
public class KafkaSource extends MessageBasedSource<Schema, GenericRecord> {

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

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newHashMap();

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    Closer closer = Closer.create();
    try {
      KafkaWrapper kafkaWrapper = closer.register(KafkaWrapper.create(state));
      List<KafkaTopic> topics = getFilteredTopics(kafkaWrapper, state);
      for (KafkaTopic topic : topics) {
        workUnits.addAll(getWorkUnitsForTopic(kafkaWrapper, topic, state));
      }
      return workUnits;
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to close kafkaWrapper", e);
      }
    }
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
        offsets.startAt(getPreviousOffsetForPartition(partition, state) + 1);
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
      KafkaPartition partition = getKafkaPartitionFromWorkUnitState(workUnitState);
      if (partition == null) {
        continue;
      }
      long previousOffset = workUnitState.getHighWaterMark();
      if (previousOffset != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
        this.previousOffsets.put(partition, previousOffset);
      }
    }
  }

  private KafkaPartition getKafkaPartitionFromWorkUnitState(WorkUnitState workUnitState) {
    if (workUnitState.getProp(TOPIC_NAME) == null || workUnitState.getProp(PARTITION_ID) == null) {
      return null;
    }
    return new KafkaPartition.Builder().withTopicName(workUnitState.getProp(TOPIC_NAME))
        .withId(workUnitState.getPropAsInt(PARTITION_ID)).build();
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
    return this.moveToLatestTopics.contains(partition.getTopicName()) || moveToLatestTopics.contains("all");
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, SourceState state, Offsets offsets) {
    SourceState partitionState = createAndPopulatePartitionState(partition, state, offsets);
    Extract extract = createExtract(partitionState, partition.getTopicName());
    return partitionState.createWorkUnit(extract);
  }

  private Extract createExtract(SourceState partitionState, String topicName) {
    TableType tableType =
        TableType.valueOf(partitionState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    String nameSpaceName = partitionState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    return partitionState.createExtract(tableType, nameSpaceName, topicName);
  }

  private SourceState createAndPopulatePartitionState(KafkaPartition partition, SourceState state, Offsets offsets) {
    SourceState partitionState = new SourceState();
    partitionState.addAll(state);
    partitionState.setProp(TOPIC_NAME, partition.getTopicName());
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
    Set<String> blacklist = state.getPropAsCaseInsensitiveSet(TOPIC_BLACKLIST);
    Set<String> whitelist = state.getPropAsCaseInsensitiveSet(TOPIC_WHITELIST);
    return kafkaWrapper.getFilteredTopics(blacklist, whitelist);
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaExtractor(state);
  }

  /**
   * This class contains startOffset, earliestOffset and latestOffset for a Kafka partition.
   *
   * @author ziliu
   *
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
