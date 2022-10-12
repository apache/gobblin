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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.gson.JsonElement;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.Watermark;
import org.apache.gobblin.source.extractor.WatermarkSerializerHelper;
import org.apache.gobblin.source.extractor.extract.FlushingExtractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.FlushRecordEnvelope;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.source.extractor.extract.kafka.KafkaProduceRateTracker.KAFKA_PARTITION_PRODUCE_RATE_KEY;
import static org.apache.gobblin.source.extractor.extract.kafka.KafkaSource.DEFAULT_GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS;
import static org.apache.gobblin.source.extractor.extract.kafka.KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS;
import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker.NUM_PARTITIONS_ASSIGNED;

/**
 * An implementation of {@link org.apache.gobblin.source.extractor.Extractor}  which reads from Kafka and returns records .
 * Type of record depends on deserializer set.
 */
@Slf4j
public class KafkaStreamingExtractor<S> extends FlushingExtractor<S, DecodeableKafkaRecord> {
  public static final String DATASET_KEY = "dataset";
  public static final String DATASET_PARTITION_KEY = "datasetPartition";
  private static final Long MAX_LOG_ERRORS = 100L;

  private static final String KAFKA_EXTRACTOR_STATS_REPORTING_INTERVAL_MINUTES_KEY =
      "gobblin.kafka.extractor.statsReportingIntervalMinutes";
  private static final Long DEFAULT_KAFKA_EXTRACTOR_STATS_REPORTING_INTERVAL_MINUTES = 1L;

  private final ClassAliasResolver<GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory>
      kafkaConsumerClientResolver;
  private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
  private final Map<String, AtomicDouble> consumerMetricsGauges = new ConcurrentHashMap<>();
  private final KafkaExtractorStatsTracker statsTracker;
  private final KafkaProduceRateTracker produceRateTracker;
  private final List<KafkaPartition> partitions;
  private final long extractorStatsReportingTimeIntervalMillis;
  //Mapping from Kafka Partition Id to partition index
  @Getter
  private final Map<Integer, Integer> partitionIdToIndexMap;
  private final String recordCreationTimestampFieldName;
  private final TimeUnit recordCreationTimestampUnit;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private long readStartTime;
  private long lastExtractorStatsReportingTime;
  private Map<KafkaPartition, Long> latestOffsetMap = Maps.newHashMap();

  protected MultiLongWatermark lowWatermark;
  protected MultiLongWatermark highWatermark;
  protected MultiLongWatermark nextWatermark;
  protected Map<Integer, DecodeableKafkaRecord> perPartitionLastSuccessfulRecord;
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  @Override
  public void shutdown() {
    this.scheduledExecutorService.shutdownNow();
    try {
      boolean shutdown = this.scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
      if (!shutdown) {
        log.error("Could not shutdown metrics collection threads in 5 seconds.");
      }
    } catch (InterruptedException e) {
      log.error("Interrupted when attempting to shutdown metrics collection threads.");
    }
    this.shutdownRequested.set(true);
    super.shutdown();
  }

  @ToString
  public static class KafkaWatermark implements CheckpointableWatermark {
    @Getter
    KafkaPartition topicPartition;
    LongWatermark _lwm;
    //Average TopicPartition Produce Rate by hour-of-day and day-of-week in records/sec.
    @Getter
    @Setter
    double[][] avgProduceRates;
    //Average consume rate for the topic when backlogged.
    @Getter
    @Setter
    double avgConsumeRate = -1.0;
    @Getter
    @Setter
    long avgRecordSize;

    @VisibleForTesting
    public KafkaWatermark(KafkaPartition topicPartition, LongWatermark lwm) {
      this.topicPartition = topicPartition;
      _lwm = lwm;
    }

    @Override
    public String getSource() {
      return topicPartition.toString();
    }

    @Override
    public ComparableWatermark getWatermark() {
      return _lwm;
    }

    @Override
    public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
      return 0;
    }

    @Override
    public JsonElement toJson() {
      return WatermarkSerializerHelper.convertWatermarkToJson(this);
    }

    @Override
    public int compareTo(CheckpointableWatermark o) {
      Preconditions.checkArgument(o instanceof KafkaWatermark);
      KafkaWatermark ko = (KafkaWatermark) o;
      Preconditions.checkArgument(topicPartition.equals(ko.topicPartition));
      return _lwm.compareTo(ko._lwm);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof KafkaWatermark)) {
        return false;
      }
      return this.compareTo((CheckpointableWatermark) obj) == 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      return topicPartition.hashCode() * prime + _lwm.hashCode();
    }

    public LongWatermark getLwm() {
      return _lwm;
    }
  }

  AtomicLong _rowCount = new AtomicLong(0);
  protected final Optional<KafkaSchemaRegistry<String, S>> _schemaRegistry;
  protected final GobblinKafkaConsumerClient kafkaConsumerClient;

  private final List<KafkaPartition> topicPartitions; // list of topic partitions assigned to this extractor

  public KafkaStreamingExtractor(WorkUnitState state) {
    super(state);
    this.kafkaConsumerClientResolver =
        new ClassAliasResolver<>(GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory.class);
    try {
      this.kafkaConsumerClient = this.closer.register(
          this.kafkaConsumerClientResolver.resolveClass(state.getProp(GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
              DEFAULT_GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS))
              .newInstance()
              .create(ConfigUtils.propertiesToConfig(state.getProperties())));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this._schemaRegistry = state.contains(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS) ? Optional.of(
        KafkaSchemaRegistry.<String, S>get(state.getProperties())) : Optional.<KafkaSchemaRegistry<String, S>>absent();

    this.topicPartitions = getTopicPartitionsFromWorkUnit(state);
    this.kafkaConsumerClient.assignAndSeek(topicPartitions, getTopicPartitionWatermarks(this.topicPartitions));
    this.messageIterator = this.kafkaConsumerClient.consume();

    this.partitions = KafkaUtils.getPartitions(state);
    this.partitionIdToIndexMap = Maps.newHashMapWithExpectedSize(this.partitions.size());
    try {
      this.latestOffsetMap = this.kafkaConsumerClient.getLatestOffsets(this.partitions);
    } catch (KafkaOffsetRetrievalFailureException e) {
      e.printStackTrace();
    }
    this.statsTracker = new KafkaExtractorStatsTracker(state, partitions);
    this.produceRateTracker = new KafkaProduceRateTracker(state, partitions, getWatermarkTracker(), statsTracker);
    this.extractorStatsReportingTimeIntervalMillis =
        state.getPropAsLong(KAFKA_EXTRACTOR_STATS_REPORTING_INTERVAL_MINUTES_KEY,
            DEFAULT_KAFKA_EXTRACTOR_STATS_REPORTING_INTERVAL_MINUTES) * 60 * 1000;
    resetExtractorStatsAndWatermarks(true);

    //Schedule a thread for reporting Kafka consumer metrics
    this.scheduledExecutorService.scheduleAtFixedRate(() -> {
      Map<String, Metric> codahaleMetricMap = kafkaConsumerClient.getMetrics();
      for (Map.Entry<String, Metric> metricEntry : codahaleMetricMap.entrySet()) {
        if (log.isDebugEnabled()) {
          log.debug("Metric name: {}, Value: {}", metricEntry.getKey(),
              ((Gauge<Double>) metricEntry.getValue()).getValue());
        }
        consumerMetricsGauges.computeIfAbsent(metricEntry.getKey(), k -> {
          AtomicDouble d = new AtomicDouble();
          ContextAwareGauge<Double> consumerMetricGauge =
              getMetricContext().newContextAwareGauge(metricEntry.getKey(), () -> d.get());
          getMetricContext().register(metricEntry.getKey(), consumerMetricGauge);
          return d;
        }).set(((Gauge<Double>) metricEntry.getValue()).getValue());
      }
    }, 0, 60, TimeUnit.SECONDS);

    this.recordCreationTimestampFieldName =
        this.workUnitState.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD, null);
    this.recordCreationTimestampUnit = TimeUnit.valueOf(
        this.workUnitState.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_UNIT, TimeUnit.MILLISECONDS.name()));
  }

  private Map<KafkaPartition, LongWatermark> getTopicPartitionWatermarks(List<KafkaPartition> topicPartitions) {
    List<String> topicPartitionStrings =
        topicPartitions.stream().map(topicPartition -> topicPartition.toString()).collect(Collectors.toList());
    // read watermarks from storage
    Map<String, CheckpointableWatermark> kafkaWatermarkMap =
        super.getCommittedWatermarks(KafkaWatermark.class, topicPartitionStrings);

    Map<KafkaPartition, LongWatermark> longWatermarkMap = new HashMap<>();
    for (KafkaPartition topicPartition : topicPartitions) {
      String topicPartitionString = topicPartition.toString();
      if (kafkaWatermarkMap.containsKey(topicPartitionString)) {
        LongWatermark longWatermark = ((KafkaWatermark) kafkaWatermarkMap.get(topicPartitionString)).getLwm();
        longWatermarkMap.put(topicPartition, longWatermark);
      } else {
        longWatermarkMap.put(topicPartition, new LongWatermark(0L));
      }
    }
    for (Map.Entry<KafkaPartition, LongWatermark> entry : longWatermarkMap.entrySet()) {
      log.info("Retrieved watermark {} for partition {}", entry.getValue().toString(), entry.getKey().toString());
    }
    return longWatermarkMap;
  }

  public static List<KafkaPartition> getTopicPartitionsFromWorkUnit(WorkUnitState state) {
    // what topic partitions are we responsible for?
    List<KafkaPartition> topicPartitions = new ArrayList<>();

    WorkUnit workUnit = state.getWorkunit();
    String topicNameProp = KafkaSource.TOPIC_NAME;
    int numOfPartitions =
        workUnit.contains(NUM_PARTITIONS_ASSIGNED) ? Integer.parseInt(workUnit.getProp(NUM_PARTITIONS_ASSIGNED)) : 0;

    for (int i = 0; i < numOfPartitions; ++i) {
      if (workUnit.getProp(topicNameProp, null) == null) {
        log.warn("There's no topic.name property being set in workunit which could be an illegal state");
        break;
      }
      String topicName = workUnit.getProp(topicNameProp);

      String partitionIdProp = KafkaSource.PARTITION_ID + "." + i;
      int partitionId = workUnit.getPropAsInt(partitionIdProp);
      KafkaPartition topicPartition = new KafkaPartition.Builder().withTopicName(topicName).withId(partitionId).build();
      topicPartitions.add(topicPartition);
    }
    return topicPartitions;
  }

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return the schema of Kafka topic being extracted
   */
  @Override
  public S getSchema() {
    try {
      if(this._schemaRegistry.isPresent()) {
        return (S)(Schema) this._schemaRegistry.get().getLatestSchemaByTopic(this.topicPartitions.get(0).getTopicName());
      }
      return (S) this.topicPartitions.iterator().next().getTopicName();
    } catch (SchemaRegistryException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    String clusterIdentifier = ClustersNames.getInstance().getClusterName();
    tags.add(new Tag<>("clusterIdentifier", clusterIdentifier));
    return tags;
  }

  /**
   * Return the next record. Return null if we're shutdown.
   */
  @SuppressWarnings("unchecked")
  @Override
  public RecordEnvelope<DecodeableKafkaRecord> readRecordEnvelopeImpl() throws IOException {
    if (this.shutdownRequested.get()) {
      return null;
    }
    this.readStartTime = System.nanoTime();
    long fetchStartTime = System.nanoTime();
    try {
      DecodeableKafkaRecord kafkaConsumerRecord;
      while(true) {
        while (this.messageIterator == null || !this.messageIterator.hasNext()) {
          Long currentTime = System.currentTimeMillis();
          //it's time to flush, so break the while loop and directly return null
          if ((currentTime - timeOfLastFlush) > this.flushIntervalMillis) {
            return new FlushRecordEnvelope();
          }
          try {
            fetchStartTime = System.nanoTime();
            this.messageIterator = this.kafkaConsumerClient.consume();
          } catch (Exception e) {
            log.error("Failed to consume from Kafka", e);
          }
        }
        kafkaConsumerRecord = (DecodeableKafkaRecord) this.messageIterator.next();
        if (kafkaConsumerRecord.getValue() != null) {
          break;
        } else {
          //Filter the null-valued records early, so that they do not break the pipeline downstream.
          if (shouldLogError()) {
            log.error("Encountered a null-valued record at offset: {}, partition: {}", kafkaConsumerRecord.getOffset(),
                kafkaConsumerRecord.getPartition());
          }
          this.statsTracker.onNullRecord(this.partitionIdToIndexMap.get(kafkaConsumerRecord.getPartition()));
        }
      }

      int partitionIndex = this.partitionIdToIndexMap.get(kafkaConsumerRecord.getPartition());
      this.statsTracker.onFetchNextMessageBuffer(partitionIndex, fetchStartTime);

      // track time for converting KafkaConsumerRecord to a RecordEnvelope
      long decodeStartTime = System.nanoTime();
      KafkaPartition topicPartition =
          new KafkaPartition.Builder().withTopicName(kafkaConsumerRecord.getTopic()).withId(kafkaConsumerRecord.getPartition()).build();
      RecordEnvelope<DecodeableKafkaRecord> recordEnvelope = new RecordEnvelope(kafkaConsumerRecord,
          new KafkaWatermark(topicPartition, new LongWatermark(kafkaConsumerRecord.getOffset())));
      recordEnvelope.setRecordMetadata("topicPartition", topicPartition);
      recordEnvelope.setRecordMetadata(DATASET_KEY, topicPartition.getTopicName());
      recordEnvelope.setRecordMetadata(DATASET_PARTITION_KEY, "" + topicPartition.getId());
      this.statsTracker.onDecodeableRecord(partitionIndex, readStartTime, decodeStartTime,
          kafkaConsumerRecord.getValueSizeInBytes(),
          kafkaConsumerRecord.isTimestampLogAppend() ? kafkaConsumerRecord.getTimestamp() : 0L,
          (this.recordCreationTimestampFieldName != null) ? kafkaConsumerRecord.getRecordCreationTimestamp(
              this.recordCreationTimestampFieldName, this.recordCreationTimestampUnit) : 0L);
      this.perPartitionLastSuccessfulRecord.put(partitionIndex, kafkaConsumerRecord);
      this.nextWatermark.set(partitionIndex, kafkaConsumerRecord.getNextOffset());
      return recordEnvelope;
    } catch (Throwable t) {
      this.statsTracker.onUndecodeableRecord(0);
      if (shouldLogError()) {
        log.error("Error when decoding a Kafka consumer record");
      }
      throw new IOException("Error in extraction", t);
    }
  }

  private boolean shouldLogError() {
    return (this.statsTracker.getUndecodableMessageCount() + this.statsTracker.getNullRecordCount()) <= MAX_LOG_ERRORS;
  }

  @Override
  protected void onFlushAck() throws IOException {
    try {
      //Refresh the latest offsets of TopicPartitions processed by the KafkaExtractor.
      this.latestOffsetMap = this.kafkaConsumerClient.getLatestOffsets(this.partitions);
    } catch (KafkaOffsetRetrievalFailureException e) {
      log.error("Unable to retrieve latest offsets due to {}", e);
    }
    long currentTime = System.currentTimeMillis();
    //Update the watermarks to include the current topic partition produce rates
    this.produceRateTracker.writeProduceRateToKafkaWatermarks(this.latestOffsetMap, getLastCommittedWatermarks(),
        this.highWatermark, currentTime);

    // Assemble additional tags to be part of GTE, for now only Partition-ProduceRate.
    Map<KafkaPartition, Map<String, String>> additionalTags = getAdditionalTagsHelper();

    //Commit offsets to the watermark storage.
    super.onFlushAck();

    //Emit GobblinTrackingEvent with current extractor stats and reset them before the next epoch starts.
    if (this.isInstrumentationEnabled()) {
      if (currentTime - this.lastExtractorStatsReportingTime > this.extractorStatsReportingTimeIntervalMillis) {
        for (int partitionIndex = 0; partitionIndex < this.partitions.size(); partitionIndex++) {
          this.statsTracker.updateStatisticsForCurrentPartition(partitionIndex, readStartTime,
              getLastSuccessfulRecordHeaderTimestamp(partitionIndex));
        }
        Map<KafkaPartition, Map<String, String>> tagsForPartitions =
            this.statsTracker.generateTagsForPartitions(lowWatermark, highWatermark, nextWatermark, additionalTags);
        this.statsTracker.emitTrackingEvents(getMetricContext(), tagsForPartitions);
        this.resetExtractorStatsAndWatermarks(false);
        this.lastExtractorStatsReportingTime = currentTime;
      }
    }
  }

  @Override
  public CommitStep initCommitStep(String commitStepAlias, boolean isPrecommit) throws IOException {
    try {
      log.info("Instantiating {}", commitStepAlias);
      return (CommitStep) GobblinConstructorUtils.invokeLongestConstructor(
          new ClassAliasResolver(CommitStep.class).resolveClass(commitStepAlias), config, statsTracker);
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }

  /**
   * A helper function to transform a Map<KafkaPartition, Double> to Map<KafkaPartition, Map<String, String>>.
   * If hard to read: Using Collectors.toMap method to construct inline-initialized Map.
   */
  Map<KafkaPartition, Map<String, String>> getAdditionalTagsHelper() {
    return produceRateTracker.getPartitionsToProdRate()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            value -> Stream.of(new AbstractMap.SimpleEntry<>(KAFKA_PARTITION_PRODUCE_RATE_KEY, value.toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  @VisibleForTesting
  public void resetExtractorStatsAndWatermarks(boolean isInit) {
    if (isInit) {
      //Initialize nextwatermark, highwatermark and lowwatermarks for Extractor stats reporting.
      this.perPartitionLastSuccessfulRecord = Maps.newHashMapWithExpectedSize(this.partitions.size());
      this.lastExtractorStatsReportingTime = System.currentTimeMillis();
      this.lowWatermark =
          new MultiLongWatermark(this.partitions.stream().map(partition -> 0L).collect(Collectors.toList()));
      this.highWatermark =
          new MultiLongWatermark(this.partitions.stream().map(partition -> 0L).collect(Collectors.toList()));
    }

    this.workUnitState.removeProp(KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME);
    this.workUnitState.removeProp(KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME);
    this.workUnitState.removeProp(KafkaSource.PREVIOUS_LOW_WATERMARK);
    this.workUnitState.removeProp(KafkaSource.PREVIOUS_HIGH_WATERMARK);
    this.workUnitState.removeProp(KafkaSource.PREVIOUS_LATEST_OFFSET);

    int partitionIndex = 0;
    for (KafkaPartition partition : partitions) {
      if (isInit) {
        this.partitionIdToIndexMap.put(partition.getId(), partitionIndex);
      }

      this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.PREVIOUS_HIGH_WATERMARK, partitionIndex),
          this.highWatermark.get(partitionIndex));
      this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.PREVIOUS_LOW_WATERMARK, partitionIndex),
          this.lowWatermark.get(partitionIndex));
      this.workUnitState.setProp(
          KafkaUtils.getPartitionPropName(KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME, partitionIndex),
          this.statsTracker.getStatsMap().get(partitions.get(partitionIndex)).getStartFetchEpochTime());
      this.workUnitState.setProp(
          KafkaUtils.getPartitionPropName(KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME, partitionIndex),
          this.statsTracker.getStatsMap().get(partitions.get(partitionIndex)).getStopFetchEpochTime());
      this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.PREVIOUS_LATEST_OFFSET, partitionIndex),
          this.highWatermark.get(partitionIndex));

      KafkaWatermark kafkaWatermark = (KafkaWatermark) this.lastCommittedWatermarks.get(partition.toString());
      long lowWatermarkValue = 0L;
      if (kafkaWatermark != null) {
        lowWatermarkValue = kafkaWatermark.getLwm().getValue() + 1;
      }
      this.lowWatermark.set(partitionIndex, lowWatermarkValue);
      if (latestOffsetMap.containsKey(partition)) {
        this.highWatermark.set(partitionIndex, latestOffsetMap.get(partition));
      }
      partitionIndex++;
    }
    this.nextWatermark = new MultiLongWatermark(this.lowWatermark);

    // Add error partition count and error message count to workUnitState
    this.workUnitState.setProp(ConfigurationKeys.ERROR_PARTITION_COUNT, this.statsTracker.getErrorPartitionCount());
    this.workUnitState.setProp(ConfigurationKeys.ERROR_MESSAGE_UNDECODABLE_COUNT,
        this.statsTracker.getUndecodableMessageCount());
    this.workUnitState.setActualHighWatermark(this.nextWatermark);

    //Reset stats tracker
    this.statsTracker.reset();
  }

  protected long getLastSuccessfulRecordHeaderTimestamp(int partitionId) {
    return 0;
  }

  /**
   * Call back that asks the extractor to remove work from its plate
   * @param workUnitState
   */
  public boolean onWorkUnitRemove(WorkUnitState workUnitState) {
    // TODO: check if these topic partitions actually were part of the assignment
    // add to queue of flush control messages
    // set up ack on them
    return false;
  }

  public boolean onWorkUnitAdd(WorkUnitState workUnitState) {
    List<KafkaPartition> newTopicPartitions = getTopicPartitionsFromWorkUnit(workUnitState);
    // get watermarks for these topic partitions
    Map<KafkaPartition, LongWatermark> topicWatermarksMap = getTopicPartitionWatermarks(newTopicPartitions);
    this.topicPartitions.addAll(newTopicPartitions);
    this.kafkaConsumerClient.assignAndSeek(topicPartitions, topicWatermarksMap);
    return true;
  }

  @Override
  public long getExpectedRecordCount() {
    return _rowCount.get();
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public String toString() {
    return topicPartitions.toString();
  }
}