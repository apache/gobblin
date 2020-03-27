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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobShutdownException;
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


  private final ClassAliasResolver<GobblinKafkaConsumerClientFactory> kafkaConsumerClientResolver;
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final String recordCreationTimestampFieldName;
  private final TimeUnit recordCreationTimestampUnit;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  @Getter
  private int currentPartitionIdx = INITIAL_PARTITION_IDX;
  @Getter
  private long readStartTime;

  protected static final int INITIAL_PARTITION_IDX = -1;

  protected static final Long MAX_LOG_DECODING_ERRORS = 5L;

  protected final WorkUnitState workUnitState;
  protected final String topicName;
  protected final List<KafkaPartition> partitions;
  protected final MultiLongWatermark lowWatermark;
  protected final MultiLongWatermark highWatermark;
  protected final MultiLongWatermark nextWatermark;
  protected final KafkaExtractorStatsTracker statsTracker;
  protected final GobblinKafkaConsumerClient kafkaConsumerClient;

  protected D currentPartitionLastSuccessfulRecord = null;

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
    this.statsTracker = new KafkaExtractorStatsTracker(state, partitions);

    // The actual high watermark starts with the low watermark
    this.workUnitState.setActualHighWatermark(this.lowWatermark);

    this.recordCreationTimestampFieldName = this.workUnitState.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_FIELD, null);
    this.recordCreationTimestampUnit = TimeUnit.valueOf(this.workUnitState.getProp(KafkaSource.RECORD_CREATION_TIMESTAMP_UNIT, TimeUnit.MILLISECONDS.name()));
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<>("kafkaTopic", KafkaUtils.getTopicName(state)));
    return tags;
  }

  protected KafkaPartition getCurrentPartition() {
    Preconditions.checkElementIndex(this.currentPartitionIdx, this.partitions.size(),
        "KafkaExtractor has finished extracting all partitions. There's no current partition.");
    return this.partitions.get(this.currentPartitionIdx);
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

    this.readStartTime = System.nanoTime();

    while (!allPartitionsFinished()) {
      if (currentPartitionFinished()) {
        moveToNextPartition();
        continue;
      }
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        try {
          long fetchStartTime = System.nanoTime();
          this.messageIterator = fetchNextMessageBuffer();
          this.statsTracker.onFetchNextMessageBuffer(this.currentPartitionIdx, fetchStartTime);
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

          this.statsTracker.onDecodeableRecord(this.currentPartitionIdx, readStartTime, decodeStartTime,
              nextValidMessage.getValueSizeInBytes(), nextValidMessage.isTimestampLogAppend() ? nextValidMessage.getTimestamp() : 0L,
              (this.recordCreationTimestampFieldName != null) ? nextValidMessage
                  .getRecordCreationTimestamp(this.recordCreationTimestampFieldName, this.recordCreationTimestampUnit) : 0L);
          this.currentPartitionLastSuccessfulRecord = record;
          return record;
        } catch (Throwable t) {
          statsTracker.onUndecodeableRecord(this.currentPartitionIdx);
          if (shouldLogError()) {
            LOG.error(String.format("A record from partition %s cannot be decoded.", getCurrentPartition()), t);
          }
        }
      }
    }
    LOG.info("Finished pulling topic " + this.topicName);
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
      LOG.info("Finished pulling partition " + getCurrentPartition());
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
      this.statsTracker.updateStatisticsForCurrentPartition(currentPartitionIdx, readStartTime, getLastSuccessfulRecordHeaderTimestamp());
      this.currentPartitionIdx++;
      this.currentPartitionLastSuccessfulRecord = null;
    }

    this.messageIterator = null;
    if (this.currentPartitionIdx < this.partitions.size()) {
      LOG.info(String.format("Pulling partition %s from offset %d to %d, range=%d", getCurrentPartition(),
          this.nextWatermark.get(this.currentPartitionIdx), this.highWatermark.get(this.currentPartitionIdx),
          this.highWatermark.get(this.currentPartitionIdx) - this.nextWatermark.get(this.currentPartitionIdx)));
      switchMetricContextToCurrentPartition();
    }

    if (!allPartitionsFinished()) {
      this.statsTracker.resetStartFetchEpochTime(currentPartitionIdx);
    }
  }

  protected long getLastSuccessfulRecordHeaderTimestamp() {
    return 0;
  }

  private void switchMetricContextToCurrentPartition() {
    if (this.currentPartitionIdx >= this.partitions.size()) {
      return;
    }
    int currentPartitionId = getCurrentPartition().getId();
    switchMetricContext(Lists.<Tag<?>> newArrayList(new Tag<>("kafka_partition", currentPartitionId)));
  }

  private Iterator<KafkaConsumerRecord> fetchNextMessageBuffer() {
    return this.kafkaConsumerClient.consume(this.partitions.get(this.currentPartitionIdx),
        this.nextWatermark.get(this.currentPartitionIdx), this.highWatermark.get(this.currentPartitionIdx));
  }

  private boolean shouldLogError() {
    return this.statsTracker.getDecodingErrorCount(this.currentPartitionIdx) <= MAX_LOG_DECODING_ERRORS;
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
    if (!allPartitionsFinished() && currentPartitionIdx != INITIAL_PARTITION_IDX) {
      this.statsTracker.updateStatisticsForCurrentPartition(currentPartitionIdx, readStartTime, getLastSuccessfulRecordHeaderTimestamp());
    }
    // Add error partition count and error message count to workUnitState
    this.workUnitState.setProp(ConfigurationKeys.ERROR_PARTITION_COUNT, this.statsTracker.getErrorPartitionCount());
    this.workUnitState.setProp(ConfigurationKeys.ERROR_MESSAGE_UNDECODABLE_COUNT, this.statsTracker.getUndecodableMessageCount());
    this.workUnitState.setActualHighWatermark(this.nextWatermark);

    // Need to call this even when not emitting metrics because some state, such as the average pull time,
    // is updated when the tags are generated
    Map<KafkaPartition, Map<String, String>> tagsForPartitionsMap = this.statsTracker.generateTagsForPartitions(
        this.lowWatermark, this.highWatermark, this.nextWatermark, Maps.newHashMap());

    if (isInstrumentationEnabled()) {
      this.statsTracker.emitTrackingEvents(getMetricContext(), tagsForPartitionsMap);
    }
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }
}
