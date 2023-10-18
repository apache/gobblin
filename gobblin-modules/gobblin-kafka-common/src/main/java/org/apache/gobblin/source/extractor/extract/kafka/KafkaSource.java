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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.extract.EventBasedSource;
import org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker;
import org.apache.gobblin.source.extractor.extract.kafka.validator.TopicValidators;
import org.apache.gobblin.source.extractor.limiter.LimiterConfigurationKeys;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.dataset.DatasetUtils;

import static java.util.stream.Collectors.toSet;


/**
 * A {@link org.apache.gobblin.source.Source} implementation for Kafka source.
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
  public static final String OFFSET_LOOKBACK = "offset_lookback";
  public static final String BOOTSTRAP_WITH_OFFSET = "bootstrap.with.offset";
  public static final String KAFKA_OFFSET_LOOKBACK = "kafka.offset.lookback";
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
  //A workunit property that contains the number of topic partitions for a given topic. Useful for
  //workunit size estimation to assign weights to a given topic partition.
  public static final String NUM_TOPIC_PARTITIONS = "numTopicPartitions";
  public static final String AVG_RECORD_MILLIS = "avg.record.millis";
  public static final String START_FETCH_EPOCH_TIME = "startFetchEpochTime";
  public static final String STOP_FETCH_EPOCH_TIME = "stopFetchEpochTime";
  public static final String PREVIOUS_START_FETCH_EPOCH_TIME = "previousStartFetchEpochTime";
  public static final String PREVIOUS_STOP_FETCH_EPOCH_TIME = "previousStopFetchEpochTime";
  public static final String PREVIOUS_LOW_WATERMARK = "previousLowWatermark";
  public static final String PREVIOUS_HIGH_WATERMARK = "previousHighWatermark";
  public static final String PREVIOUS_LATEST_OFFSET = "previousLatestOffset";
  public static final String OFFSET_FETCH_EPOCH_TIME = "offsetFetchEpochTime";
  public static final String PREVIOUS_OFFSET_FETCH_EPOCH_TIME = "previousOffsetFetchEpochTime";
  public static final String ALLOW_PERIOD_IN_TOPIC_NAME = "gobblin.kafka.allowPeriodInTopicName";
  public static final String GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS = "gobblin.kafka.consumerClient.class";
  public static final String GOBBLIN_KAFKA_EXTRACT_ALLOW_TABLE_TYPE_NAMESPACE_CUSTOMIZATION =
      "gobblin.kafka.extract.allowTableTypeAndNamspaceCustomization";
  public static final String DEFAULT_GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS =
      "org.apache.gobblin.kafka.client.Kafka08ConsumerClient$Factory";
  public static final String GOBBLIN_KAFKA_SHOULD_ENABLE_DATASET_STATESTORE =
      "gobblin.kafka.shouldEnableDatasetStateStore";
  public static final boolean DEFAULT_GOBBLIN_KAFKA_SHOULD_ENABLE_DATASET_STATESTORE = false;
  public static final String OFFSET_FETCH_TIMER = "offsetFetchTimer";
  public static final String RECORD_LEVEL_SLA_MINUTES_KEY = "gobblin.kafka.recordLevelSlaMinutes";
  public static final String MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS = "gobblin.kafka.maxobservedLatencyInHours";
  public static final Integer DEFAULT_MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS = 24;
  public static final String OBSERVED_LATENCY_PRECISION = "gobblin.kafka.observedLatencyPrecision";
  public static final Integer DEFAULT_OBSERVED_LATENCY_PRECISION = 3;
  public static final String OBSERVED_LATENCY_MEASUREMENT_ENABLED = "gobblin.kafka.observedLatencyMeasurementEnabled";
  public static final Boolean DEFAULT_OBSERVED_LATENCY_MEASUREMENT_ENABLED = false;
  public static final String RECORD_CREATION_TIMESTAMP_FIELD = "gobblin.kafka.recordCreationTimestampField";
  public static final String RECORD_CREATION_TIMESTAMP_UNIT = "gobblin.kafka.recordCreationTimestampUnit";

  private final Set<String> moveToLatestTopics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
  private final Map<KafkaPartition, Long> previousOffsets = Maps.newConcurrentMap();
  private final Map<KafkaPartition, Long> previousLowWatermarks = Maps.newConcurrentMap();
  private final Map<KafkaPartition, Long> previousExpectedHighWatermarks = Maps.newConcurrentMap();
  private final Map<KafkaPartition, Long> previousOffsetFetchEpochTimes = Maps.newConcurrentMap();
  private final Map<KafkaPartition, Long> previousStartFetchEpochTimes = Maps.newConcurrentMap();
  private final Map<KafkaPartition, Long> previousStopFetchEpochTimes = Maps.newConcurrentMap();

  private final Set<KafkaPartition> partitionsToBeProcessed = Sets.newConcurrentHashSet();

  private final AtomicInteger failToGetOffsetCount = new AtomicInteger(0);
  private final AtomicInteger offsetTooEarlyCount = new AtomicInteger(0);
  private final AtomicInteger offsetTooLateCount = new AtomicInteger(0);

  // sharing the kafka consumer may result in contention, so support thread local consumers
  protected final ConcurrentLinkedQueue<GobblinKafkaConsumerClient> kafkaConsumerClientPool = new ConcurrentLinkedQueue();
  protected static final ThreadLocal<GobblinKafkaConsumerClient> kafkaConsumerClient =
          new ThreadLocal<GobblinKafkaConsumerClient>();
  private GobblinKafkaConsumerClient sharedKafkaConsumerClient = null;
  private final ClassAliasResolver<GobblinKafkaConsumerClientFactory> kafkaConsumerClientResolver =
      new ClassAliasResolver<>(GobblinKafkaConsumerClientFactory.class);

  private volatile boolean doneGettingAllPreviousOffsets = false;
  private Extract.TableType tableType;
  private String extractNamespace;
  private boolean isFullExtract;
  private String kafkaBrokers;
  private boolean shouldEnableDatasetStateStore;
  private AtomicBoolean isDatasetStateEnabled = new AtomicBoolean(false);
  private Set<String> topicsToProcess;

  private MetricContext metricContext;

  protected Optional<LineageInfo> lineageInfo;

  private List<String> getLimiterExtractorReportKeys() {
    List<String> keyNames = new ArrayList<>();
    keyNames.add(KafkaSource.TOPIC_NAME);
    keyNames.add(KafkaSource.PARTITION_ID);
    return keyNames;
  }

  private void setLimiterReportKeyListToWorkUnits(List<WorkUnit> workUnits, List<String> keyNameList) {
    if (keyNameList.isEmpty()) {
      return;
    }
    String keyList = Joiner.on(',').join(keyNameList.iterator());
    for (WorkUnit workUnit : workUnits) {
      workUnit.setProp(LimiterConfigurationKeys.LIMITER_REPORT_KEY_LIST, keyList);
    }
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    this.metricContext = Instrumented.getMetricContext(state, KafkaSource.class);
    this.lineageInfo = LineageInfo.getLineageInfo(state.getBroker());

    Map<String, List<WorkUnit>> workUnits = Maps.newConcurrentMap();
    if (state.getPropAsBoolean(KafkaSource.GOBBLIN_KAFKA_EXTRACT_ALLOW_TABLE_TYPE_NAMESPACE_CUSTOMIZATION)) {
      String tableTypeStr =
          state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, KafkaSource.DEFAULT_TABLE_TYPE.toString());
      tableType = Extract.TableType.valueOf(tableTypeStr);
      extractNamespace =
          state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, KafkaSource.DEFAULT_NAMESPACE_NAME);
    } else {
      // To be compatible, reject table type and namespace configuration keys as previous implementation
      tableType = KafkaSource.DEFAULT_TABLE_TYPE;
      extractNamespace = KafkaSource.DEFAULT_NAMESPACE_NAME;
    }
    isFullExtract = state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY);
    kafkaBrokers = state.getProp(ConfigurationKeys.KAFKA_BROKERS, "");
    this.shouldEnableDatasetStateStore = state.getPropAsBoolean(GOBBLIN_KAFKA_SHOULD_ENABLE_DATASET_STATESTORE,
        DEFAULT_GOBBLIN_KAFKA_SHOULD_ENABLE_DATASET_STATESTORE);

    try {
      Config config = ConfigUtils.propertiesToConfig(state.getProperties());
      GobblinKafkaConsumerClientFactory kafkaConsumerClientFactory = kafkaConsumerClientResolver
              .resolveClass(
                      state.getProp(GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
                              DEFAULT_GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS)).newInstance();

      this.kafkaConsumerClient.set(kafkaConsumerClientFactory.create(config));

      List<KafkaTopic> topics = getValidTopics(getFilteredTopics(state), state);
      this.topicsToProcess = topics.stream().map(KafkaTopic::getName).collect(toSet());

      for (String topic : this.topicsToProcess) {
        LOG.info("Discovered topic " + topic);
      }
      Map<String, State> topicSpecificStateMap =
          DatasetUtils.getDatasetSpecificProps(Iterables.transform(topics, new Function<KafkaTopic, String>() {

            @Override
            public String apply(KafkaTopic topic) {
              return topic.getName();
            }
          }), state);

      for (KafkaTopic topic : topics) {
        if (topic.getTopicSpecificState().isPresent()) {
          topicSpecificStateMap.computeIfAbsent(topic.getName(), k -> new State())
              .addAllIfNotExist(topic.getTopicSpecificState().get());
        }
      }

      int numOfThreads = state.getPropAsInt(ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS,
          ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT);
      ExecutorService threadPool =
          Executors.newFixedThreadPool(numOfThreads, ExecutorsUtils.newThreadFactory(Optional.of(LOG)));

      if (state.getPropAsBoolean(ConfigurationKeys.KAFKA_SOURCE_SHARE_CONSUMER_CLIENT,
              ConfigurationKeys.DEFAULT_KAFKA_SOURCE_SHARE_CONSUMER_CLIENT)) {
        this.sharedKafkaConsumerClient = this.kafkaConsumerClient.get();
      } else {
        // preallocate one client per thread
        populateClientPool(numOfThreads, kafkaConsumerClientFactory, config);
      }

      Stopwatch createWorkUnitStopwatch = Stopwatch.createStarted();

      for (KafkaTopic topic : topics) {
        threadPool.submit(
            new WorkUnitCreator(topic, state, Optional.fromNullable(topicSpecificStateMap.get(topic.getName())),
                workUnits));
      }

      ExecutorsUtils.shutdownExecutorService(threadPool, Optional.of(LOG), 1L, TimeUnit.HOURS);
      LOG.info(String.format("Created workunits for %d topics in %d seconds", workUnits.size(),
          createWorkUnitStopwatch.elapsed(TimeUnit.SECONDS)));

      // Create empty WorkUnits for skipped partitions (i.e., partitions that have previous offsets,
      // but aren't processed).
      createEmptyWorkUnitsForSkippedPartitions(workUnits, topicSpecificStateMap, state);
      //determine the number of mappers
      int maxMapperNum =
          state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);
      KafkaWorkUnitPacker kafkaWorkUnitPacker = KafkaWorkUnitPacker.getInstance(this, state, Optional.of(this.metricContext));
      int numOfMultiWorkunits = maxMapperNum;
      if(state.contains(ConfigurationKeys.MR_TARGET_MAPPER_SIZE)) {
        double totalEstDataSize = kafkaWorkUnitPacker.setWorkUnitEstSizes(workUnits);
        LOG.info(String.format("The total estimated data size is %.2f", totalEstDataSize));
        double targetMapperSize = state.getPropAsDouble(ConfigurationKeys.MR_TARGET_MAPPER_SIZE);
        numOfMultiWorkunits = (int) (totalEstDataSize / targetMapperSize) + 1;
        numOfMultiWorkunits = Math.min(numOfMultiWorkunits, maxMapperNum);
      }
      addTopicSpecificPropsToWorkUnits(workUnits, topicSpecificStateMap);
      List<WorkUnit> workUnitList = kafkaWorkUnitPacker.pack(workUnits, numOfMultiWorkunits);
      setLimiterReportKeyListToWorkUnits(workUnitList, getLimiterExtractorReportKeys());
      return workUnitList;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException("Checked exception caught", e);
    } catch (Throwable t) {
      throw new RuntimeException("Unexpected throwable caught, ", t);
    } finally {
      try {
        GobblinKafkaConsumerClient consumerClient = this.kafkaConsumerClient.get();
        if (consumerClient != null) {
          consumerClient.close();
        }
        // cleanup clients from pool
        for (GobblinKafkaConsumerClient client: kafkaConsumerClientPool) {
          client.close();
        }
      } catch (Throwable t) {
        //Swallow any exceptions in the finally{..} block to allow potential exceptions from the main try{..} block to be
        //propagated
        LOG.error("Exception {} encountered closing GobblinKafkaConsumerClient ", t);
      }
    }
  }

  protected void populateClientPool(int count,
      GobblinKafkaConsumerClientFactory kafkaConsumerClientFactory,
      Config config) {
    for (int i = 0; i < count; i++) {
      kafkaConsumerClientPool.offer(kafkaConsumerClientFactory.create(config));
    }
  }

  private void addTopicSpecificPropsToWorkUnits(Map<String, List<WorkUnit>> workUnits, Map<String, State> topicSpecificStateMap) {
    for (List<WorkUnit> workUnitList : workUnits.values()) {
      for (WorkUnit workUnit : workUnitList) {
        addTopicSpecificPropsToWorkUnit(workUnit, topicSpecificStateMap);
      }
    }
  }

  private void addTopicSpecificPropsToWorkUnit(WorkUnit workUnit, Map<String, State> topicSpecificStateMap) {
    if (workUnit instanceof MultiWorkUnit) {
      for (WorkUnit wu : ((MultiWorkUnit) workUnit).getWorkUnits()) {
        addTopicSpecificPropsToWorkUnit(wu, topicSpecificStateMap);
      }
    } else if (!workUnit.contains(TOPIC_NAME)) {
      return;
    } else {
      addDatasetUrnOptionally(workUnit);
      if (topicSpecificStateMap == null) {
        return;
      } else if (!topicSpecificStateMap.containsKey(workUnit.getProp(TOPIC_NAME))) {
        return;
      } else {
        workUnit.addAll(topicSpecificStateMap.get(workUnit.getProp(TOPIC_NAME)));
      }
    }
  }

  private void addDatasetUrnOptionally(WorkUnit workUnit) {
    if (!this.shouldEnableDatasetStateStore) {
      return;
    }
    workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, workUnit.getProp(TOPIC_NAME));
  }

  private void createEmptyWorkUnitsForSkippedPartitions(Map<String, List<WorkUnit>> workUnits,
      Map<String, State> topicSpecificStateMap, SourceState state) {

    // in case the previous offset not been set
    getAllPreviousOffsetState(state);

    // For each partition that has a previous offset, create an empty WorkUnit for it if
    // it is not in this.partitionsToBeProcessed.
    for (Map.Entry<KafkaPartition, Long> entry : this.previousOffsets.entrySet()) {
      KafkaPartition partition = entry.getKey();
      if (!this.partitionsToBeProcessed.contains(partition)) {
        String topicName = partition.getTopicName();
        if (!this.isDatasetStateEnabled.get() || this.topicsToProcess.contains(topicName)) {
          long previousOffset = entry.getValue();
          WorkUnit emptyWorkUnit = createEmptyWorkUnit(partition, previousOffset,
              this.previousOffsetFetchEpochTimes.get(partition),
              Optional.fromNullable(topicSpecificStateMap.get(partition.getTopicName())));

          if (workUnits.containsKey(topicName)) {
            workUnits.get(topicName).add(emptyWorkUnit);
          } else {
            workUnits.put(topicName, Lists.newArrayList(emptyWorkUnit));
          }
        }
      }
    }
  }

  /*
   * This function need to be thread safe since it is called in the Runnable
   */
  private List<WorkUnit> getWorkUnitsForTopic(KafkaTopic topic, SourceState state, Optional<State> topicSpecificState) {
    Timer.Context context = this.metricContext.timer("isTopicQualifiedTimer").time();
    boolean topicQualified = isTopicQualified(topic);
    context.close();

    List<WorkUnit> workUnits = Lists.newArrayList();
    List<KafkaPartition> topicPartitions = topic.getPartitions();
    for (KafkaPartition partition : topicPartitions) {
      WorkUnit workUnit = getWorkUnitForTopicPartition(partition, state, topicSpecificState);
      if (workUnit != null) {
        // For disqualified topics, for each of its workunits set the high watermark to be the same
        // as the low watermark, so that it will be skipped.
        if (!topicQualified) {
          skipWorkUnit(workUnit);
        }
        workUnit.setProp(NUM_TOPIC_PARTITIONS, topicPartitions.size());
        workUnits.add(workUnit);
      }
    }
    this.partitionsToBeProcessed.addAll(topic.getPartitions());
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

    try (Timer.Context context = this.metricContext.timer(OFFSET_FETCH_TIMER).time()) {
      offsets.setOffsetFetchEpochTime(System.currentTimeMillis());
      offsets.setEarliestOffset(this.kafkaConsumerClient.get().getEarliestOffset(partition));
      offsets.setLatestOffset(this.kafkaConsumerClient.get().getLatestOffset(partition));
    } catch (Throwable t) {
      failedToGetKafkaOffsets = true;
      LOG.error("Caught error in creating work unit for {}", partition, t);
    }

    long previousOffset = 0;
    long previousOffsetFetchEpochTime = 0;
    boolean previousOffsetNotFound = false;
    try {
      previousOffset = getPreviousOffsetForPartition(partition, state);
      offsets.setPreviousEndOffset(previousOffset);
      offsets.setPreviousStartOffset(getPreviousLowWatermark(partition, state));
      offsets.setPreviousStartFetchEpochTime(getPreviousStartFetchEpochTimeForPartition(partition, state));
      offsets.setPreviousStopFetchEpochTime(getPreviousStopFetchEpochTimeForPartition(partition, state));
      offsets.setPreviousLatestOffset(getPreviousExpectedHighWatermark(partition, state));
      previousOffsetFetchEpochTime = getPreviousOffsetFetchEpochTimeForPartition(partition, state);
      offsets.setPreviousOffsetFetchEpochTime(previousOffsetFetchEpochTime);
    } catch (PreviousOffsetNotFoundException e) {
      previousOffsetNotFound = true;
    }

    if (failedToGetKafkaOffsets) {

      // Increment counts, which will be reported as job metrics
      this.failToGetOffsetCount.incrementAndGet();

      // When unable to get earliest/latest offsets from Kafka, skip the partition and create an empty workunit,
      // so that previousOffset is persisted.
      LOG.warn(String
          .format("Failed to retrieve earliest and/or latest offset for partition %s. This partition will be skipped.",
              partition));
      return previousOffsetNotFound ? null : createEmptyWorkUnit(partition, previousOffset, previousOffsetFetchEpochTime,
          topicSpecificState);
    }

    if (shouldMoveToLatestOffset(partition, state)) {
      offsets.startAtLatestOffset();
    } else if (previousOffsetNotFound) {

      /**
       * When previous offset cannot be found, either start at earliest offset, latest offset, go back with (latest - lookback)
       * (long value to be deducted from latest offset in order to avoid data loss) or skip the partition
       * (no need to create an empty workunit in this case since there's no offset to persist).
       * In case of no previous state OFFSET_LOOKBACK will make sure to avoid consuming huge amount of data (earlist) and data loss (latest offset)
       * lookback can be set to any long value where (latest-lookback) is nearest offset for each partition. If computed offset is out of range then
       * partition will be consumed from latest offset
       **/
      String offsetNotFoundMsg = String.format("Previous offset for partition %s does not exist. ", partition);
      String offsetOption = state.getProp(BOOTSTRAP_WITH_OFFSET, DEFAULT_BOOTSTRAP_WITH_OFFSET).toLowerCase();
      if (offsetOption.equals(LATEST_OFFSET)) {
        LOG.warn(offsetNotFoundMsg + "This partition will start from the latest offset: " + offsets.getLatestOffset());
        offsets.startAtLatestOffset();
      } else if (offsetOption.equals(EARLIEST_OFFSET)) {
        LOG.warn(
            offsetNotFoundMsg + "This partition will start from the earliest offset: " + offsets.getEarliestOffset());
        offsets.startAtEarliestOffset();
      } else if (offsetOption.equals(OFFSET_LOOKBACK)) {
        long lookbackOffsetRange = state.getPropAsLong(KAFKA_OFFSET_LOOKBACK , 0L);
        long latestOffset = offsets.getLatestOffset();
        long offset = latestOffset - lookbackOffsetRange;
        LOG.warn(offsetNotFoundMsg + "This partition will start from latest-lookback [ " + latestOffset + " - " + lookbackOffsetRange + " ]  start offset: " + offset);
        try {
          offsets.startAt(offset);
        } catch (StartOffsetOutOfRangeException e) {
          // Increment counts, which will be reported as job metrics
          if (offsets.getStartOffset() <= offsets.getLatestOffset()) {
            this.offsetTooEarlyCount.incrementAndGet();
          } else {
            this.offsetTooLateCount.incrementAndGet();
          }

          // When above computed offset (latest-lookback) is out of range, either start at earliest, latest or nearest offset, or skip the
          // partition. If skipping, need to create an empty workunit so that previousOffset is persisted.
          String offsetOutOfRangeMsg = String.format(
                  "Start offset for partition %s is out of range. Start offset = %d, earliest offset = %d, latest offset = %d.",
                  partition, offsets.getStartOffset(), offsets.getEarliestOffset(), offsets.getLatestOffset());
          offsetOption =
                  state.getProp(RESET_ON_OFFSET_OUT_OF_RANGE, DEFAULT_RESET_ON_OFFSET_OUT_OF_RANGE).toLowerCase();
          if (offsetOption.equals(LATEST_OFFSET) || (offsetOption.equals(NEAREST_OFFSET)
                  && offsets.getStartOffset() >= offsets.getLatestOffset())) {
            LOG.warn(
                    offsetOutOfRangeMsg + "This partition will start from the latest offset: " + offsets.getLatestOffset());
            offsets.startAtLatestOffset();
          } else if (offsetOption.equals(EARLIEST_OFFSET) || offsetOption.equals(NEAREST_OFFSET)) {
            LOG.warn(offsetOutOfRangeMsg + "This partition will start from the earliest offset: " + offsets
                    .getEarliestOffset());
            offsets.startAtEarliestOffset();
          } else {
            LOG.warn(offsetOutOfRangeMsg + "This partition will be skipped.");
            return createEmptyWorkUnit(partition, previousOffset, previousOffsetFetchEpochTime, topicSpecificState);
          }
        }
      }
      else {
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
        if (offsetOption.equals(LATEST_OFFSET) || (offsetOption.equals(NEAREST_OFFSET)
            && offsets.getStartOffset() >= offsets.getLatestOffset())) {
          LOG.warn(
              offsetOutOfRangeMsg + "This partition will start from the latest offset: " + offsets.getLatestOffset());
          offsets.startAtLatestOffset();
        } else if (offsetOption.equals(EARLIEST_OFFSET) || offsetOption.equals(NEAREST_OFFSET)) {
          LOG.warn(offsetOutOfRangeMsg + "This partition will start from the earliest offset: " + offsets
              .getEarliestOffset());
          offsets.startAtEarliestOffset();
        } else {
          LOG.warn(offsetOutOfRangeMsg + "This partition will be skipped.");
          return createEmptyWorkUnit(partition, previousOffset, previousOffsetFetchEpochTime, topicSpecificState);
        }
      }
    }
    WorkUnit workUnit = getWorkUnitForTopicPartition(partition, offsets, topicSpecificState);
    addSourceStatePropsToWorkUnit(workUnit, state);
    return workUnit;
  }

  /**
   * A method to copy specific properties from the {@link SourceState} object to {@link WorkUnitState}.
   * @param workUnit WorkUnit state
   * @param state Source state
   */
  private void addSourceStatePropsToWorkUnit(WorkUnit workUnit, SourceState state) {
    //Copy the SLA config from SourceState to WorkUnitState.
    if (state.contains(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY)) {
      workUnit.setProp(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY, state.getProp(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY));
    }
    boolean isobservedLatencyMeasurementEnabled = state.getPropAsBoolean(KafkaSource.OBSERVED_LATENCY_MEASUREMENT_ENABLED, DEFAULT_OBSERVED_LATENCY_MEASUREMENT_ENABLED);
    if (isobservedLatencyMeasurementEnabled) {
      Preconditions.checkArgument(state.contains(KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD), "Missing config key: " + KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD);
      workUnit.setProp(KafkaSource.OBSERVED_LATENCY_MEASUREMENT_ENABLED, isobservedLatencyMeasurementEnabled);
      workUnit.setProp(KafkaSource.MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS,
          state.getPropAsInt(KafkaSource.MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS, DEFAULT_MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS));
      workUnit.setProp(KafkaSource.OBSERVED_LATENCY_PRECISION,
          state.getPropAsInt(KafkaSource.OBSERVED_LATENCY_PRECISION, KafkaSource.DEFAULT_OBSERVED_LATENCY_PRECISION));
      workUnit.setProp(KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD, state.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD));
      workUnit.setProp(KafkaSource.RECORD_CREATION_TIMESTAMP_UNIT, state.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_UNIT, TimeUnit.MILLISECONDS.name()));
    }
    if (state.contains(ConfigurationKeys.JOB_NAME_KEY)) {
      workUnit.setProp(ConfigurationKeys.JOB_NAME_KEY, state.getProp(ConfigurationKeys.JOB_NAME_KEY));
    }
    if (state.contains(ConfigurationKeys.JOB_ID_KEY)) {
      workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, state.getProp(ConfigurationKeys.JOB_ID_KEY));
    }
  }

  private long getPreviousStartFetchEpochTimeForPartition(KafkaPartition partition, SourceState state) {
    getAllPreviousOffsetState(state);
    return this.previousStartFetchEpochTimes.containsKey(partition) ?
        this.previousStartFetchEpochTimes.get(partition) : 0;
  }

  private long getPreviousStopFetchEpochTimeForPartition(KafkaPartition partition, SourceState state) {
    getAllPreviousOffsetState(state);
    return this.previousStopFetchEpochTimes.containsKey(partition) ?
        this.previousStopFetchEpochTimes.get(partition) : 0;
  }

  private long getPreviousOffsetFetchEpochTimeForPartition(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {

    getAllPreviousOffsetState(state);

    if (this.previousOffsetFetchEpochTimes.containsKey(partition)) {
      return this.previousOffsetFetchEpochTimes.get(partition);
    }

    throw new PreviousOffsetNotFoundException(String
        .format("Previous offset fetch epoch time for topic %s, partition %s not found.", partition.getTopicName(),
            partition.getId()));
  }

  private long getPreviousOffsetForPartition(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {

    getAllPreviousOffsetState(state);

    if (this.previousOffsets.containsKey(partition)) {
      return this.previousOffsets.get(partition);
    }
    throw new PreviousOffsetNotFoundException(String
        .format("Previous offset for topic %s, partition %s not found.", partition.getTopicName(), partition.getId()));
  }

  private long getPreviousExpectedHighWatermark(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {

    getAllPreviousOffsetState(state);

    if (this.previousExpectedHighWatermarks.containsKey(partition)) {
      return this.previousExpectedHighWatermarks.get(partition);
    }
    throw new PreviousOffsetNotFoundException(String
        .format("Previous expected high watermark for topic %s, partition %s not found.", partition.getTopicName(),
            partition.getId()));
  }

  private long getPreviousLowWatermark(KafkaPartition partition, SourceState state)
      throws PreviousOffsetNotFoundException {

    getAllPreviousOffsetState(state);

    if (this.previousLowWatermarks.containsKey(partition)) {
      return this.previousLowWatermarks.get(partition);
    }
    throw new PreviousOffsetNotFoundException(String
        .format("Previous low watermark for topic %s, partition %s not found.", partition.getTopicName(),
            partition.getId()));
  }

  // need to be synchronized as this.previousOffsets, this.previousExpectedHighWatermarks, and
  // this.previousOffsetFetchEpochTimes need to be initialized once
  private synchronized void getAllPreviousOffsetState(SourceState state) {
    if (this.doneGettingAllPreviousOffsets) {
      return;
    }
    this.previousOffsets.clear();
    this.previousLowWatermarks.clear();
    this.previousExpectedHighWatermarks.clear();
    this.previousOffsetFetchEpochTimes.clear();
    this.previousStartFetchEpochTimes.clear();
    this.previousStopFetchEpochTimes.clear();
    Map<String, Iterable<WorkUnitState>> workUnitStatesByDatasetUrns = state.getPreviousWorkUnitStatesByDatasetUrns();

    if (!workUnitStatesByDatasetUrns.isEmpty() &&
            !(workUnitStatesByDatasetUrns.size() == 1 && workUnitStatesByDatasetUrns.keySet().iterator().next()
        .equals(""))) {
      this.isDatasetStateEnabled.set(true);
    }

    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      WorkUnit workUnit = workUnitState.getWorkunit();

      MultiLongWatermark watermark = workUnitState.getActualHighWatermark(MultiLongWatermark.class);
      MultiLongWatermark previousLowWatermark = workUnit.getLowWatermark(MultiLongWatermark.class);
      MultiLongWatermark previousExpectedHighWatermark = workUnit.getExpectedHighWatermark(MultiLongWatermark.class);
      Preconditions.checkArgument(partitions.size() == watermark.size(), String
          .format("Num of partitions doesn't match number of watermarks: partitions=%s, watermarks=%s", partitions,
              watermark));

      for (int i = 0; i < partitions.size(); i++) {
        KafkaPartition partition = partitions.get(i);

        if (watermark.get(i) != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
          this.previousOffsets.put(partition, watermark.get(i));
        }

        if (previousLowWatermark.get(i) != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
          this.previousLowWatermarks.put(partition, previousLowWatermark.get(i));
        }

        if (previousExpectedHighWatermark.get(i) != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
          this.previousExpectedHighWatermarks.put(partition, previousExpectedHighWatermark.get(i));
        }

        this.previousOffsetFetchEpochTimes.put(partition,
            KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(workUnitState, OFFSET_FETCH_EPOCH_TIME, i));

        this.previousStartFetchEpochTimes.put(partition,
            KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(workUnitState, START_FETCH_EPOCH_TIME, i));

        this.previousStopFetchEpochTimes.put(partition,
            KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(workUnitState, STOP_FETCH_EPOCH_TIME, i));
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
  private WorkUnit createEmptyWorkUnit(KafkaPartition partition, long previousOffset, long previousFetchEpochTime,
      Optional<State> topicSpecificState) {
    Offsets offsets = new Offsets();
    offsets.setEarliestOffset(previousOffset);
    offsets.setLatestOffset(previousOffset);
    offsets.startAtEarliestOffset();
    offsets.setOffsetFetchEpochTime(previousFetchEpochTime);
    return getWorkUnitForTopicPartition(partition, offsets, topicSpecificState);
  }

  private WorkUnit getWorkUnitForTopicPartition(KafkaPartition partition, Offsets offsets,
      Optional<State> topicSpecificState) {
    // Default to job level configurations
    Extract.TableType currentTableType = tableType;
    String currentExtractNamespace = extractNamespace;
    String currentExtractTableName = partition.getTopicName();
    boolean isCurrentFullExtract = isFullExtract;
    // Update to topic specific configurations if any
    if (topicSpecificState.isPresent()) {
      State topicState = topicSpecificState.get();
      if (topicState.contains(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY)) {
        currentTableType = Extract.TableType.valueOf(topicState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY));
      }
      currentExtractNamespace = topicState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, extractNamespace);
      currentExtractTableName =
          topicState.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, partition.getTopicName());
      isCurrentFullExtract = topicState.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, isFullExtract);
    }

    Extract extract = this.createExtract(currentTableType, currentExtractNamespace, currentExtractTableName);
    if (isCurrentFullExtract) {
      extract.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
    }

    WorkUnit workUnit = WorkUnit.create(extract);
    workUnit.setProp(TOPIC_NAME, partition.getTopicName());
    addDatasetUrnOptionally(workUnit);
    workUnit.setProp(PARTITION_ID, partition.getId());
    workUnit.setProp(LEADER_ID, partition.getLeader().getId());
    workUnit.setProp(LEADER_HOSTANDPORT, partition.getLeader().getHostAndPort().toString());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, offsets.getStartOffset());
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, offsets.getLatestOffset());
    workUnit.setProp(PREVIOUS_START_FETCH_EPOCH_TIME, offsets.getPreviousStartFetchEpochTime());
    workUnit.setProp(PREVIOUS_STOP_FETCH_EPOCH_TIME, offsets.getPreviousStopFetchEpochTime());
    workUnit.setProp(PREVIOUS_LOW_WATERMARK, offsets.getPreviousStartOffset());
    workUnit.setProp(PREVIOUS_HIGH_WATERMARK, offsets.getPreviousEndOffset());
    workUnit.setProp(PREVIOUS_OFFSET_FETCH_EPOCH_TIME, offsets.getPreviousOffsetFetchEpochTime());
    workUnit.setProp(OFFSET_FETCH_EPOCH_TIME, offsets.getOffsetFetchEpochTime());
    workUnit.setProp(PREVIOUS_LATEST_OFFSET, offsets.getPreviousLatestOffset());

    // Add lineage info
    DatasetDescriptor source = new DatasetDescriptor(DatasetConstants.PLATFORM_KAFKA, partition.getTopicName());
    source.addMetadata(DatasetConstants.BROKERS, kafkaBrokers);
    if (this.lineageInfo.isPresent()) {
      this.lineageInfo.get().setSource(source, workUnit);
    }

    LOG.info(String.format("Created workunit for partition %s: lowWatermark=%d, highWatermark=%d, range=%d", partition,
        offsets.getStartOffset(), offsets.getLatestOffset(), offsets.getLatestOffset() - offsets.getStartOffset()));

    return workUnit;
  }

  /**
   * Return topics to be processed filtered by job-level whitelist and blacklist.
   */
  protected List<KafkaTopic> getFilteredTopics(SourceState state) {
    List<Pattern> blacklist = DatasetFilterUtils.getPatternList(state, TOPIC_BLACKLIST);
    List<Pattern> whitelist = DatasetFilterUtils.getPatternList(state, TOPIC_WHITELIST);
    // TODO: replace this with TopicNameValidator in the config once TopicValidators is rolled out.
    if (!state.getPropAsBoolean(KafkaSource.ALLOW_PERIOD_IN_TOPIC_NAME, true)) {
      blacklist.add(Pattern.compile(".*\\..*"));
    }
    return kafkaConsumerClient.get().getFilteredTopics(blacklist, whitelist);
  }

  @Override
  public void shutdown(SourceState state) {
    state.setProp(ConfigurationKeys.OFFSET_TOO_EARLY_COUNT, this.offsetTooEarlyCount);
    state.setProp(ConfigurationKeys.OFFSET_TOO_LATE_COUNT, this.offsetTooLateCount);
    state.setProp(ConfigurationKeys.FAIL_TO_GET_OFFSET_COUNT, this.failToGetOffsetCount);
  }

  /**
   * Return topics that pass all the topic validators.
   */
  protected List<KafkaTopic> getValidTopics(List<KafkaTopic> topics, SourceState state) {
    return new TopicValidators(state).validate(topics);
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

    @Getter
    @Setter
    private long offsetFetchEpochTime = 0;

    @Getter
    @Setter
    private long previousOffsetFetchEpochTime = 0;

    @Getter
    @Setter
    private long previousLatestOffset = 0;

    // previous low watermark
    @Getter
    @Setter
    private long previousStartOffset = 0;

    // previous actual high watermark
    @Getter
    @Setter
    private long previousEndOffset = 0;

    @Getter
    @Setter
    private long previousStartFetchEpochTime = 0;

    @Getter
    @Setter
    private long previousStopFetchEpochTime = 0;

    private void startAt(long offset)
        throws StartOffsetOutOfRangeException {
      if (offset < this.earliestOffset || offset > this.latestOffset) {
        throw new StartOffsetOutOfRangeException(String
            .format("start offset = %d, earliest offset = %d, latest offset = %d", offset, this.earliestOffset,
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
    public static final String WORK_UNITS_FOR_TOPIC_TIMER = "workUnitsForTopicTimer";
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
      try (Timer.Context context = metricContext.timer(WORK_UNITS_FOR_TOPIC_TIMER).time()) {
        // use shared client if configure, otherwise set a thread local one from the pool
        if (KafkaSource.this.sharedKafkaConsumerClient != null) {
          KafkaSource.this.kafkaConsumerClient.set(KafkaSource.this.sharedKafkaConsumerClient);
        } else {
          GobblinKafkaConsumerClient client = KafkaSource.this.kafkaConsumerClientPool.poll();
          Preconditions.checkNotNull(client, "Unexpectedly ran out of preallocated consumer clients");
          KafkaSource.this.kafkaConsumerClient.set(client);
        }

        this.allTopicWorkUnits.put(this.topic.getName(),
            KafkaSource.this.getWorkUnitsForTopic(this.topic, this.state, this.topicSpecificState));
      } catch (Throwable t) {
        LOG.error("Caught error in creating work unit for " + this.topic.getName(), t);
        throw new RuntimeException(t);
      } finally {
        // return the client to the pool
        if (KafkaSource.this.sharedKafkaConsumerClient == null) {
          KafkaSource.this.kafkaConsumerClientPool.offer(KafkaSource.this.kafkaConsumerClient.get());
          KafkaSource.this.kafkaConsumerClient.remove();
        }
      }
    }
  }
}
