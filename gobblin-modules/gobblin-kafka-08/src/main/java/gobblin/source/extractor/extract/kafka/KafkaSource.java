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

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.collect.Iterables;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.EventBasedSource;
import gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.dataset.DatasetUtils;


/**
 * A {@link gobblin.source.Source} implementation for Kafka source.
 *
 * @author Ziyang Liu
 */
public abstract class KafkaSource<S, D> extends EventBasedSource<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

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
  public static final String AVG_RECORD_SIZE = "avg.record.size";
  public static final String AVG_RECORD_MILLIS = "avg.record.millis";

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newConcurrentMap();

  private final Set<KafkaPartition> partitionsToBeProcessed = Sets.newConcurrentHashSet();

  private final Closer closer = Closer.create();

  private final AtomicInteger failToGetOffsetCount = new AtomicInteger(0);
  private final AtomicInteger offsetTooEarlyCount = new AtomicInteger(0);
  private final AtomicInteger offsetTooLateCount = new AtomicInteger(0);

  private KafkaWrapper kafkaWrapper;

  private volatile boolean doneGettingAllPreviousOffsets = false;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Map<String, List<WorkUnit>> workUnits = Maps.newConcurrentMap();

    this.kafkaWrapper = this.closer.register(KafkaWrapper.create(state));

    List<KafkaTopic> topics = getFilteredTopics(state);

    for (KafkaTopic topic: topics)
    {
      LOG.info("Discovered topic " + topic.getName());
    }
    Map<String, State> topicSpecificStateMap =
        DatasetUtils.getDatasetSpecificProps(Iterables.transform(topics, new Function<KafkaTopic, String>() {

          @Override
          public String apply(KafkaTopic topic) {
            return topic.getName();
          }
        }), state);

    int numOfThreads = state.getPropAsInt(ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS,
        ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT);
    ExecutorService threadPool =
        Executors.newFixedThreadPool(numOfThreads, ExecutorsUtils.newThreadFactory(Optional.of(LOG)));

    Stopwatch createWorkUnitStopwatch = Stopwatch.createStarted();

    for (KafkaTopic topic : topics) {
      threadPool.submit(new WorkUnitCreator(topic, state,
          Optional.fromNullable(topicSpecificStateMap.get(topic.getName())), workUnits));
    }

    ExecutorsUtils.shutdownExecutorService(threadPool, Optional.of(LOG), 1L, TimeUnit.HOURS);
    LOG.info(String.format("Created workunits for %d topics in %d seconds", workUnits.size(),
        createWorkUnitStopwatch.elapsed(TimeUnit.SECONDS)));

    // Create empty WorkUnits for skipped partitions (i.e., partitions that have previous offsets,
    // but aren't processed).
    createEmptyWorkUnitsForSkippedPartitions(workUnits, topicSpecificStateMap, state);

    int numOfMultiWorkunits =
        state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);
    return KafkaWorkUnitPacker.getInstance(this, state).pack(workUnits, numOfMultiWorkunits);
  }

  private void createEmptyWorkUnitsForSkippedPartitions(Map<String, List<WorkUnit>> workUnits,
      Map<String, State> topicSpecificStateMap, SourceState state) {

    // in case the previous offset not been set
    getAllPreviousOffsets(state);

    // For each partition that has a previous offset, create an empty WorkUnit for it if
    // it is not in this.partitionsToBeProcessed.
    for (Map.Entry<KafkaPartition, Long> entry : this.previousOffsets.entrySet()) {
      KafkaPartition partition = entry.getKey();

      if (!this.partitionsToBeProcessed.contains(partition)) {
        long previousOffset = entry.getValue();
        WorkUnit emptyWorkUnit = createEmptyWorkUnit(partition, previousOffset,
            Optional.fromNullable(topicSpecificStateMap.get(partition.getTopicName())));
        String topicName = partition.getTopicName();
        if (workUnits.containsKey(topicName)) {
          workUnits.get(topicName).add(emptyWorkUnit);
        } else {
          workUnits.put(topicName, Lists.newArrayList(emptyWorkUnit));
        }
      }
    }
  }

  /*
   * This function need to be thread safe since it is called in the Runnable
   */
  private List<WorkUnit> getWorkUnitsForTopic(KafkaTopic topic, SourceState state, Optional<State> topicSpecificState) {
    boolean topicQualified = isTopicQualified(topic);

    List<WorkUnit> workUnits = Lists.newArrayList();
    for (KafkaPartition partition : topic.getPartitions()) {
      WorkUnit workUnit = getWorkUnitForTopicPartition(partition, state, topicSpecificState);
      this.partitionsToBeProcessed.add(partition);
      if (workUnit != null) {

        // For disqualified topics, for each of its workunits set the high watermark to be the same
        // as the low watermark, so that it will be skipped.
        if (!topicQualified) {
          skipWorkUnit(workUnit);
        }
        workUnits.add(workUnit);
      }
    }
    return workUnits;
  }

  /**
   * Whether a {@link KafkaTopic} is qualified to be pulled.
   *
   * This method can be overridden by subclasses for verifying topic eligibility, e.g., one may want to
   * skip a topic if its schema cannot be found in the schema registry.
   */
  protected boolean isTopicQualified(KafkaTopic topic) {
    return true;
  }

  @SuppressWarnings("deprecation")
  private static void skipWorkUnit(WorkUnit workUnit) {
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, workUnit.getLowWaterMark());
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, SourceState state,
      Optional<State> topicSpecificState) {
    Offsets offsets = new Offsets();

    boolean failedToGetKafkaOffsets = false;

    try {
      offsets.setEarliestOffset(this.kafkaWrapper.getEarliestOffset(partition));
      offsets.setLatestOffset(this.kafkaWrapper.getLatestOffset(partition));
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
      this.failToGetOffsetCount.incrementAndGet();

      // When unable to get earliest/latest offsets from Kafka, skip the partition and create an empty workunit,
      // so that previousOffset is persisted.
      LOG.warn(String.format(
          "Failed to retrieve earliest and/or latest offset for partition %s. This partition will be skipped.",
          partition));
      return previousOffsetNotFound ? null : createEmptyWorkUnit(partition, previousOffset, topicSpecificState);
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
          this.offsetTooEarlyCount.incrementAndGet();
        } else {
          this.offsetTooLateCount.incrementAndGet();
        }

        // When previous offset is out of range, either start at earliest, latest or nearest offset, or skip the
        // partition. If skipping, need to create an empty workunit so that previousOffset is persisted.
        String offsetOutOfRangeMsg = String.format(
            "Start offset for partition %s is out of range. Start offset = %d, earliest offset = %d, latest offset = %d.",
            partition, offsets.getStartOffset(), offsets.getEarliestOffset(), offsets.getLatestOffset());
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
          return createEmptyWorkUnit(partition, previousOffset, topicSpecificState);
        }
      }
    }

    return getWorkUnitForTopicPartition(partition, offsets, topicSpecificState);
  }

  private long getPreviousOffsetForPartition(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {

    getAllPreviousOffsets(state);

    if (this.previousOffsets.containsKey(partition)) {
      return this.previousOffsets.get(partition);
    }
    throw new PreviousOffsetNotFoundException(String.format("Previous offset for topic %s, partition %s not found.",
        partition.getTopicName(), partition.getId()));
  }

  // need to be synchronized as this.previousOffsets need to be initialized once
  private synchronized void getAllPreviousOffsets(SourceState state) {
    if (this.doneGettingAllPreviousOffsets) {
      return;
    }

    this.previousOffsets.clear();
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      MultiLongWatermark watermark = workUnitState.getActualHighWatermark(MultiLongWatermark.class);
      Preconditions.checkArgument(partitions.size() == watermark.size(), String.format(
          "Num of partitions doesn't match number of watermarks: partitions=%s, watermarks=%s", partitions, watermark));
      for (int i = 0; i < partitions.size(); i++) {
        if (watermark.get(i) != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
          this.previousOffsets.put(partitions.get(i), watermark.get(i));
        }
      }
    }

    this.doneGettingAllPreviousOffsets = true;
  }

  /**
   * A topic can be configured to move to the latest offset in {@link #TOPICS_MOVE_TO_LATEST_OFFSET}.
   *
   * Need to be synchronized as access by multiple threads
   */
  private synchronized boolean shouldMoveToLatestOffset(KafkaPartition partition, SourceState state) {
    if (!state.contains(TOPICS_MOVE_TO_LATEST_OFFSET)) {
      return false;
    }
    if (this.moveToLatestTopics.isEmpty()) {
      this.moveToLatestTopics.addAll(
          Splitter.on(',').trimResults().omitEmptyStrings().splitToList(state.getProp(TOPICS_MOVE_TO_LATEST_OFFSET)));
    }
    return this.moveToLatestTopics.contains(partition.getTopicName()) || this.moveToLatestTopics.contains(ALL_TOPICS);
  }

  // thread safe
  private WorkUnit createEmptyWorkUnit(KafkaPartition partition, long previousOffset,
      Optional<State> topicSpecificState) {
    Offsets offsets = new Offsets();
    offsets.setEarliestOffset(previousOffset);
    offsets.setLatestOffset(previousOffset);
    offsets.startAtEarliestOffset();
    return getWorkUnitForTopicPartition(partition, offsets, topicSpecificState);
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, Offsets offsets,
      Optional<State> topicSpecificState) {
    Extract extract = this.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, partition.getTopicName());
    WorkUnit workUnit = WorkUnit.create(extract);
    if (topicSpecificState.isPresent()) {
      workUnit.addAll(topicSpecificState.get());
    }
    workUnit.setProp(TOPIC_NAME, partition.getTopicName());
    workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partition.getTopicName());
    workUnit.setProp(PARTITION_ID, partition.getId());
    workUnit.setProp(LEADER_ID, partition.getLeader().getId());
    workUnit.setProp(LEADER_HOSTANDPORT, partition.getLeader().getHostAndPort().toString());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, offsets.getStartOffset());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, offsets.getLatestOffset());
    LOG.info(String.format("Created workunit for partition %s: lowWatermark=%d, highWatermark=%d, range=%d", partition,
        offsets.getStartOffset(), offsets.getLatestOffset(), offsets.getLatestOffset() - offsets.getStartOffset()));
    return workUnit;
  }

  private List<KafkaTopic> getFilteredTopics(SourceState state) {
    List<Pattern> blacklist = DatasetFilterUtils.getPatternList(state, TOPIC_BLACKLIST);
    List<Pattern> whitelist = DatasetFilterUtils.getPatternList(state, TOPIC_WHITELIST);
    return this.kafkaWrapper.getFilteredTopics(blacklist, whitelist);
  }

  @Override
  public void shutdown(SourceState state) {
    state.setProp(ConfigurationKeys.OFFSET_TOO_EARLY_COUNT, this.offsetTooEarlyCount);
    state.setProp(ConfigurationKeys.OFFSET_TOO_LATE_COUNT, this.offsetTooLateCount);
    state.setProp(ConfigurationKeys.FAIL_TO_GET_OFFSET_COUNT, this.failToGetOffsetCount);

    try {
      this.closer.close();
    } catch (IOException e) {
      LOG.error("Failed to close kafkaWrapper", e);
    }
  }

  /**
   * This class contains startOffset, earliestOffset and latestOffset for a Kafka partition.
   */
  private static class Offsets {

    @Getter
    private long startOffset = 0;

    @Getter
    @Setter
    private long earliestOffset = 0;

    @Getter
    @Setter
    private long latestOffset = 0;

    private void startAt(long offset) throws StartOffsetOutOfRangeException {
      if (offset < this.earliestOffset || offset > this.latestOffset) {
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
  }

  private class WorkUnitCreator implements Runnable {
    private final KafkaTopic topic;
    private final SourceState state;
    private final Optional<State> topicSpecificState;
    private final Map<String, List<WorkUnit>> allTopicWorkUnits;

    WorkUnitCreator(KafkaTopic topic, SourceState state, Optional<State> topicSpecificState,
        Map<String, List<WorkUnit>> workUnits) {
      this.topic = topic;
      this.state = state;
      this.topicSpecificState = topicSpecificState;
      this.allTopicWorkUnits = workUnits;
    }

    @Override
    public void run() {
      try {
        this.allTopicWorkUnits.put(this.topic.getName(),
            KafkaSource.this.getWorkUnitsForTopic(this.topic, this.state, this.topicSpecificState));
      } catch (Throwable t) {
        LOG.error("Caught error in creating work unit for " + this.topic.getName(), t);
        throw t;
      }
    }

  }
}
