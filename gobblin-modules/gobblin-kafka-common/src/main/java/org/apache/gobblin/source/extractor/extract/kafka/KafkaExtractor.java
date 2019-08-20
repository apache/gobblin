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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.gobblin.runtime.JobShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.EventBasedExtractor;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link Extractor} for Apache Kafka. Each {@link KafkaExtractor} processes
 * one or more partitions of the same topic.
 *
 * @author Ziyang Liu
 */
public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExtractor.class);

  protected static final int INITIAL_PARTITION_IDX = -1;
  protected static final Long MAX_LOG_DECODING_ERRORS = 5L;

  // Constants for event submission
  public static final String TOPIC = "topic";
  public static final String PARTITION = "partition";
  public static final String LOW_WATERMARK = "lowWatermark";
  public static final String ACTUAL_HIGH_WATERMARK = "actualHighWatermark";
  public static final String EXPECTED_HIGH_WATERMARK = "expectedHighWatermark";
  public static final String ELAPSED_TIME = "elapsedTime";
  public static final String PROCESSED_RECORD_COUNT = "processedRecordCount";
  public static final String UNDECODABLE_MESSAGE_COUNT = "undecodableMessageCount";
  public static final String PARTITION_TOTAL_SIZE = "partitionTotalSize";
  public static final String AVG_RECORD_PULL_TIME = "avgRecordPullTime";
  public static final String READ_RECORD_TIME = "readRecordTime";
  public static final String DECODE_RECORD_TIME = "decodeRecordTime";
  public static final String FETCH_MESSAGE_BUFFER_TIME = "fetchMessageBufferTime";
  public static final String GOBBLIN_KAFKA_NAMESPACE = "gobblin.kafka";
  public static final String KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME = "KafkaExtractorTopicMetadata";

  protected final WorkUnitState workUnitState;
  protected final String topicName;
  protected final List<KafkaPartition> partitions;
  protected final MultiLongWatermark lowWatermark;
  protected final MultiLongWatermark highWatermark;
  protected final MultiLongWatermark nextWatermark;
  protected final GobblinKafkaConsumerClient kafkaConsumerClient;
  private final ClassAliasResolver<GobblinKafkaConsumerClientFactory> kafkaConsumerClientResolver;

  protected final Map<KafkaPartition, Long> decodingErrorCount;
  private final Map<KafkaPartition, Double> avgMillisPerRecord;
  private final Map<KafkaPartition, Long> avgRecordSizes;
  private final Map<KafkaPartition, Long> elapsedTime;
  private final Map<KafkaPartition, Long> processedRecordCount;
  private final Map<KafkaPartition, Long> partitionTotalSize;
  private final Map<KafkaPartition, Long> decodeRecordTime;
  private final Map<KafkaPartition, Long> fetchMessageBufferTime;
  private final Map<KafkaPartition, Long> readRecordTime;
  private final Map<KafkaPartition, Long> startFetchEpochTime;
  private final Map<KafkaPartition, Long> stopFetchEpochTime;

  private final Set<Integer> errorPartitions;
  private int undecodableMessageCount = 0;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private int currentPartitionIdx = INITIAL_PARTITION_IDX;
  private long currentPartitionRecordCount = 0;
  private long currentPartitionTotalSize = 0;
  private long currentPartitionDecodeRecordTime = 0;
  private long currentPartitionFetchMessageBufferTime = 0;
  private long currentPartitionReadRecordTime = 0;
  protected D currentPartitionLastSuccessfulRecord = null;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  public KafkaExtractor(WorkUnitState state) {
    super(state);
    this.workUnitState = state;
    this.topicName = KafkaUtils.getTopicName(state);
    this.partitions = KafkaUtils.getPartitions(state);
    this.lowWatermark = state.getWorkunit().getLowWatermark(MultiLongWatermark.class);
    this.highWatermark = state.getWorkunit().getExpectedHighWatermark(MultiLongWatermark.class);
    this.nextWatermark = new MultiLongWatermark(this.lowWatermark);
    this.kafkaConsumerClientResolver = new ClassAliasResolver<>(GobblinKafkaConsumerClientFactory.class);
    try {
      this.kafkaConsumerClient =
          this.closer.register(this.kafkaConsumerClientResolver
              .resolveClass(
                  state.getProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
                      KafkaSource.DEFAULT_GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS)).newInstance()
              .create(ConfigUtils.propertiesToConfig(state.getProperties())));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.decodingErrorCount = Maps.newHashMap();
    this.avgMillisPerRecord = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.avgRecordSizes = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.elapsedTime = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.processedRecordCount = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.partitionTotalSize = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.decodeRecordTime = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.fetchMessageBufferTime = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.readRecordTime = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.startFetchEpochTime = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.stopFetchEpochTime= Maps.newHashMapWithExpectedSize(this.partitions.size());

    this.errorPartitions = Sets.newHashSet();

    // The actual high watermark starts with the low watermark
    this.workUnitState.setActualHighWatermark(this.lowWatermark);
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<>("kafkaTopic", KafkaUtils.getTopicName(state)));
    return tags;
  }

  /**
   * Return the next decodable record from the current partition. If the current partition has no more
   * decodable record, move on to the next partition. If all partitions have been processed, return null.
   */
  @SuppressWarnings("unchecked")
  @Override
  public D readRecordImpl(D reuse) throws DataRecordException, IOException {
    if (this.shutdownRequested.get()) {
      return null;
    }

    long readStartTime = System.nanoTime();

    while (!allPartitionsFinished()) {
      if (currentPartitionFinished()) {
        moveToNextPartition();
        continue;
      }
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        try {
          long fetchStartTime = System.nanoTime();
          this.messageIterator = fetchNextMessageBuffer();
          this.currentPartitionFetchMessageBufferTime += System.nanoTime() - fetchStartTime;
        } catch (Exception e) {
          LOG.error(String.format("Failed to fetch next message buffer for partition %s. Will skip this partition.",
              getCurrentPartition()), e);
          moveToNextPartition();
          continue;
        }
        if (this.messageIterator == null || !this.messageIterator.hasNext()) {
          moveToNextPartition();
          continue;
        }
      }
      while (!currentPartitionFinished()) {
        if (!this.messageIterator.hasNext()) {
          break;
        }

        KafkaConsumerRecord nextValidMessage = this.messageIterator.next();

        // Even though we ask Kafka to give us a message buffer starting from offset x, it may
        // return a buffer that starts from offset smaller than x, so we need to skip messages
        // until we get to x.
        if (nextValidMessage.getOffset() < this.nextWatermark.get(this.currentPartitionIdx)) {
          continue;
        }

        this.nextWatermark.set(this.currentPartitionIdx, nextValidMessage.getNextOffset());
        try {
          // track time for decode/convert depending on the record type
          long decodeStartTime = System.nanoTime();

          D record = decodeKafkaMessage(nextValidMessage);

          this.currentPartitionDecodeRecordTime += System.nanoTime() - decodeStartTime;
          this.currentPartitionRecordCount++;
          this.currentPartitionTotalSize += nextValidMessage.getValueSizeInBytes();
          this.currentPartitionReadRecordTime += System.nanoTime() - readStartTime;
          this.currentPartitionLastSuccessfulRecord = record;
          return record;
        } catch (Throwable t) {
          this.errorPartitions.add(this.currentPartitionIdx);
          this.undecodableMessageCount++;
          if (shouldLogError()) {
            LOG.error(String.format("A record from partition %s cannot be decoded.", getCurrentPartition()), t);
          }
          incrementErrorCount();
        }
      }
    }
    LOG.info("Finished pulling topic " + this.topicName);

    this.currentPartitionReadRecordTime += System.nanoTime() - readStartTime;
    return null;
  }

  protected D decodeKafkaMessage(KafkaConsumerRecord message) throws DataRecordException, IOException {

    D record = null;

    if (message instanceof ByteArrayBasedKafkaRecord) {
      record = decodeRecord((ByteArrayBasedKafkaRecord)message);
    } else if (message instanceof DecodeableKafkaRecord){
      // if value is null then this is a bad record that is returned for further error handling, so raise an error
      if (((DecodeableKafkaRecord) message).getValue() == null) {
        throw new DataRecordException("Could not decode Kafka record");
      }

      // get value from decodeable record and convert to the output schema if necessary
      record = convertRecord(((DecodeableKafkaRecord<?, D>) message).getValue());
    } else {
      throw new IllegalStateException(
          "Unsupported KafkaConsumerRecord type. The returned record can either be ByteArrayBasedKafkaRecord"
              + " or DecodeableKafkaRecord");
    }

    return record;
  }

  @Override
  public void shutdown()
      throws JobShutdownException {
    this.shutdownRequested.set(true);
  }

  private boolean allPartitionsFinished() {
    return this.currentPartitionIdx != INITIAL_PARTITION_IDX && this.currentPartitionIdx >= this.highWatermark.size();
  }

  private boolean currentPartitionFinished() {
    if (this.currentPartitionIdx == INITIAL_PARTITION_IDX) {
      return true;
    } else if (this.nextWatermark.get(this.currentPartitionIdx) >= this.highWatermark.get(this.currentPartitionIdx)) {
      LOG.info("Finished pulling partition " + this.getCurrentPartition());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Record the avg time per record for the current partition, then increment this.currentPartitionIdx,
   * and switch metric context to the new partition.
   */
  private void moveToNextPartition() {
    if (this.currentPartitionIdx == INITIAL_PARTITION_IDX) {
      LOG.info("Pulling topic " + this.topicName);
      this.currentPartitionIdx = 0;
    } else {
      updateStatisticsForCurrentPartition();
      this.currentPartitionIdx++;
      this.currentPartitionRecordCount = 0;
      this.currentPartitionTotalSize = 0;
      this.currentPartitionDecodeRecordTime = 0;
      this.currentPartitionFetchMessageBufferTime = 0;
      this.currentPartitionReadRecordTime = 0;
      this.currentPartitionLastSuccessfulRecord = null;
    }

    this.messageIterator = null;
    if (this.currentPartitionIdx < this.partitions.size()) {
      LOG.info(String.format("Pulling partition %s from offset %d to %d, range=%d", this.getCurrentPartition(),
          this.nextWatermark.get(this.currentPartitionIdx), this.highWatermark.get(this.currentPartitionIdx),
          this.highWatermark.get(this.currentPartitionIdx) - this.nextWatermark.get(this.currentPartitionIdx)));
      switchMetricContextToCurrentPartition();
    }

    if (!allPartitionsFinished()) {
      this.startFetchEpochTime.put(this.getCurrentPartition(), System.currentTimeMillis());
    }
  }

  protected void updateStatisticsForCurrentPartition() {
    long stopFetchEpochTime = System.currentTimeMillis();

    if (!allPartitionsFinished()) {
      this.stopFetchEpochTime.put(this.getCurrentPartition(), stopFetchEpochTime);
    }

    if (this.currentPartitionRecordCount != 0) {
      long currentPartitionFetchDuration =
          stopFetchEpochTime - this.startFetchEpochTime.get(this.getCurrentPartition());
      double avgMillisForCurrentPartition =
          (double) currentPartitionFetchDuration / (double) this.currentPartitionRecordCount;
      this.avgMillisPerRecord.put(this.getCurrentPartition(), avgMillisForCurrentPartition);

      long avgRecordSize = this.currentPartitionTotalSize / this.currentPartitionRecordCount;
      this.avgRecordSizes.put(this.getCurrentPartition(), avgRecordSize);

      this.elapsedTime.put(this.getCurrentPartition(), currentPartitionFetchDuration);
      this.processedRecordCount.put(this.getCurrentPartition(), this.currentPartitionRecordCount);
      this.partitionTotalSize.put(this.getCurrentPartition(), this.currentPartitionTotalSize);
      this.decodeRecordTime.put(this.getCurrentPartition(), this.currentPartitionDecodeRecordTime);
      this.fetchMessageBufferTime.put(this.getCurrentPartition(), this.currentPartitionFetchMessageBufferTime);
      this.readRecordTime.put(this.getCurrentPartition(), this.currentPartitionReadRecordTime);
    }
  }

  private void switchMetricContextToCurrentPartition() {
    if (this.currentPartitionIdx >= this.partitions.size()) {
      return;
    }
    int currentPartitionId = this.getCurrentPartition().getId();
    switchMetricContext(Lists.<Tag<?>> newArrayList(new Tag<>("kafka_partition", currentPartitionId)));
  }

  private Iterator<KafkaConsumerRecord> fetchNextMessageBuffer() {
    return this.kafkaConsumerClient.consume(this.partitions.get(this.currentPartitionIdx),
        this.nextWatermark.get(this.currentPartitionIdx), this.highWatermark.get(this.currentPartitionIdx));
  }

  private boolean shouldLogError() {
    return !this.decodingErrorCount.containsKey(getCurrentPartition())
        || this.decodingErrorCount.get(getCurrentPartition()) <= MAX_LOG_DECODING_ERRORS;
  }

  private void incrementErrorCount() {
    if (this.decodingErrorCount.containsKey(getCurrentPartition())) {
      this.decodingErrorCount.put(getCurrentPartition(), this.decodingErrorCount.get(getCurrentPartition()) + 1);
    } else {
      this.decodingErrorCount.put(getCurrentPartition(), 1L);
    }
  }

  protected KafkaPartition getCurrentPartition() {
    Preconditions.checkElementIndex(this.currentPartitionIdx, this.partitions.size(),
        "KafkaExtractor has finished extracting all partitions. There's no current partition.");
    return this.partitions.get(this.currentPartitionIdx);
  }

  protected abstract D decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException;

  /**
   * Convert a record to the output format
   * @param record the input record
   * @return the converted record
   * @throws IOException
   */
  protected D convertRecord(D record) throws IOException {
    // default implementation does no conversion
    return record;
  }

  @Override
  public long getExpectedRecordCount() {
    return this.lowWatermark.getGap(this.highWatermark);
  }

  @Override
  public void close() throws IOException {
    if (currentPartitionIdx != INITIAL_PARTITION_IDX) {
      updateStatisticsForCurrentPartition();
    }

    Map<KafkaPartition, Map<String, String>> tagsForPartitionsMap = Maps.newHashMap();

    // Add error partition count and error message count to workUnitState
    this.workUnitState.setProp(ConfigurationKeys.ERROR_PARTITION_COUNT, this.errorPartitions.size());
    this.workUnitState.setProp(ConfigurationKeys.ERROR_MESSAGE_UNDECODABLE_COUNT, this.undecodableMessageCount);

    for (int i = 0; i < this.partitions.size(); i++) {
      LOG.info(String.format("Actual high watermark for partition %s=%d, expected=%d", this.partitions.get(i),
          this.nextWatermark.get(i), this.highWatermark.get(i)));
      tagsForPartitionsMap.put(this.partitions.get(i), createTagsForPartition(i));
    }
    this.workUnitState.setActualHighWatermark(this.nextWatermark);

    if (isInstrumentationEnabled()) {
      for (Map.Entry<KafkaPartition, Map<String, String>> eventTags : tagsForPartitionsMap.entrySet()) {
        new EventSubmitter.Builder(getMetricContext(), GOBBLIN_KAFKA_NAMESPACE).build()
            .submit(KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME, eventTags.getValue());
      }
    }

    this.closer.close();
  }

  protected Map<String, String> createTagsForPartition(int partitionId) {
    Map<String, String> tagsForPartition = Maps.newHashMap();
    KafkaPartition partition = this.partitions.get(partitionId);

    tagsForPartition.put(TOPIC, partition.getTopicName());
    tagsForPartition.put(PARTITION, Integer.toString(partition.getId()));
    tagsForPartition.put(LOW_WATERMARK, Long.toString(this.lowWatermark.get(partitionId)));
    tagsForPartition.put(ACTUAL_HIGH_WATERMARK, Long.toString(this.nextWatermark.get(partitionId)));

    // These are used to compute the load factor,
    // gobblin consumption rate relative to the kafka production rate.
    // The gobblin rate is computed as (processed record count/elapsed time)
    // The kafka rate is computed as (expected high watermark - previous latest offset) /
    // (current offset fetch epoch time - previous offset fetch epoch time).
    tagsForPartition.put(EXPECTED_HIGH_WATERMARK, Long.toString(this.highWatermark.get(partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_OFFSET_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_OFFSET_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.OFFSET_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.OFFSET_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_LATEST_OFFSET,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_LATEST_OFFSET, partitionId)));

    tagsForPartition.put(KafkaSource.PREVIOUS_LOW_WATERMARK,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_LOW_WATERMARK, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_HIGH_WATERMARK,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_HIGH_WATERMARK, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME, partitionId)));

    tagsForPartition.put(KafkaSource.START_FETCH_EPOCH_TIME, Long.toString(this.startFetchEpochTime.getOrDefault(partition, 0L)));
    tagsForPartition.put(KafkaSource.STOP_FETCH_EPOCH_TIME, Long.toString(this.stopFetchEpochTime.getOrDefault(partition, 0L)));
    this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.START_FETCH_EPOCH_TIME, partitionId),
        Long.toString(this.startFetchEpochTime.getOrDefault(partition, 0L)));
    this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.STOP_FETCH_EPOCH_TIME, partitionId),
        Long.toString(this.stopFetchEpochTime.getOrDefault(partition, 0L)));

    if (this.processedRecordCount.containsKey(partition)) {
      tagsForPartition.put(PROCESSED_RECORD_COUNT, Long.toString(this.processedRecordCount.get(partition)));
      tagsForPartition.put(PARTITION_TOTAL_SIZE, Long.toString(this.partitionTotalSize.get(partition)));
      tagsForPartition.put(ELAPSED_TIME, Long.toString(this.elapsedTime.get(partition)));
      tagsForPartition.put(DECODE_RECORD_TIME, Long.toString(TimeUnit.NANOSECONDS.toMillis(
          this.decodeRecordTime.get(partition))));
      tagsForPartition.put(FETCH_MESSAGE_BUFFER_TIME, Long.toString(TimeUnit.NANOSECONDS.toMillis(
          this.fetchMessageBufferTime.get(partition))));
      tagsForPartition.put(READ_RECORD_TIME, Long.toString(TimeUnit.NANOSECONDS.toMillis(
          this.readRecordTime.get(partition))));
    } else {
      tagsForPartition.put(PROCESSED_RECORD_COUNT, "0");
      tagsForPartition.put(PARTITION_TOTAL_SIZE, "0");
      tagsForPartition.put(ELAPSED_TIME, "0");
      tagsForPartition.put(DECODE_RECORD_TIME, "0");
      tagsForPartition.put(FETCH_MESSAGE_BUFFER_TIME, "0");
      tagsForPartition.put(READ_RECORD_TIME, "0");
    }

    tagsForPartition.put(UNDECODABLE_MESSAGE_COUNT,
        Long.toString(this.decodingErrorCount.getOrDefault(partition, 0L)));

    // Commit avg time to pull a record for each partition
    if (this.avgMillisPerRecord.containsKey(partition)) {
      double avgMillis = this.avgMillisPerRecord.get(partition);
      LOG.info(String.format("Avg time to pull a record for partition %s = %f milliseconds", partition, avgMillis));
      KafkaUtils.setPartitionAvgRecordMillis(this.workUnitState, partition, avgMillis);
      tagsForPartition.put(AVG_RECORD_PULL_TIME, Double.toString(avgMillis));
    } else {
      LOG.info(String.format("Avg time to pull a record for partition %s not recorded", partition));
      tagsForPartition.put(AVG_RECORD_PULL_TIME, Double.toString(-1));
    }

    return tagsForPartition;
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }
}
