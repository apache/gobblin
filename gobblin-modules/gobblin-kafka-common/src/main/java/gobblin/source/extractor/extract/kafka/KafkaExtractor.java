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

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.kafka.client.DecodeableKafkaRecord;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.kafka.client.GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory;
import gobblin.kafka.client.KafkaConsumerRecord;
import gobblin.metrics.Tag;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.EventBasedExtractor;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;


/**
 * An implementation of {@link Extractor} for Apache Kafka. Each {@link KafkaExtractor} processes
 * one or more partitions of the same topic.
 *
 * @author Ziyang Liu
 */
public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExtractor.class);

  protected static final int INITIAL_PARTITION_IDX = -1;
  protected static final Integer MAX_LOG_DECODING_ERRORS = 5;

  protected final WorkUnitState workUnitState;
  protected final String topicName;
  protected final List<KafkaPartition> partitions;
  protected final MultiLongWatermark lowWatermark;
  protected final MultiLongWatermark highWatermark;
  protected final MultiLongWatermark nextWatermark;
  protected final GobblinKafkaConsumerClient kafkaConsumerClient;
  private final ClassAliasResolver<GobblinKafkaConsumerClientFactory> kafkaConsumerClientResolver;

  protected final Stopwatch stopwatch;

  protected final Map<KafkaPartition, Integer> decodingErrorCount;
  private final Map<KafkaPartition, Double> avgMillisPerRecord;
  private final Map<KafkaPartition, Long> avgRecordSizes;

  private final Set<Integer> errorPartitions;
  private int undecodableMessageCount = 0;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private int currentPartitionIdx = INITIAL_PARTITION_IDX;
  private long currentPartitionRecordCount = 0;
  private long currentPartitionTotalSize = 0;

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

    this.stopwatch = Stopwatch.createUnstarted();

    this.decodingErrorCount = Maps.newHashMap();
    this.avgMillisPerRecord = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.avgRecordSizes = Maps.newHashMapWithExpectedSize(this.partitions.size());

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
    while (!allPartitionsFinished()) {
      if (currentPartitionFinished()) {
        moveToNextPartition();
        continue;
      }
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        try {
          this.messageIterator = fetchNextMessageBuffer();
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
          D record = null;
          if (nextValidMessage instanceof ByteArrayBasedKafkaRecord) {
            record = decodeRecord((ByteArrayBasedKafkaRecord)nextValidMessage);
          } else if (nextValidMessage instanceof DecodeableKafkaRecord){
            // get value from decodeable record and convert to the output schema if necessary
            record = convertRecord(((DecodeableKafkaRecord<?, D>) nextValidMessage).getValue());
          } else {
            throw new IllegalStateException(
                "Unsupported KafkaConsumerRecord type. The returned record can either be ByteArrayBasedKafkaRecord"
                    + " or DecodeableKafkaRecord");
          }

          this.currentPartitionRecordCount++;
          this.currentPartitionTotalSize += nextValidMessage.getValueSizeInBytes();
          return record;
        } catch (Throwable t) {
          this.errorPartitions.add(this.currentPartitionIdx);
          this.undecodableMessageCount++;
          if (shouldLogError()) {
            LOG.error(String.format("A record from partition %s cannot be decoded.", getCurrentPartition()), t);
            incrementErrorCount();
          }
        }
      }
    }
    LOG.info("Finished pulling topic " + this.topicName);
    return null;
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
      this.stopwatch.stop();
      if (this.currentPartitionRecordCount != 0) {
        double avgMillisForCurrentPartition =
            (double) this.stopwatch.elapsed(TimeUnit.MILLISECONDS) / (double) this.currentPartitionRecordCount;
        this.avgMillisPerRecord.put(this.getCurrentPartition(), avgMillisForCurrentPartition);

        long avgRecordSize = this.currentPartitionTotalSize / this.currentPartitionRecordCount;
        this.avgRecordSizes.put(this.getCurrentPartition(), avgRecordSize);
      }
      this.currentPartitionIdx++;
      this.currentPartitionRecordCount = 0;
      this.currentPartitionTotalSize = 0;
      this.stopwatch.reset();
    }

    this.messageIterator = null;
    if (this.currentPartitionIdx < this.partitions.size()) {
      LOG.info(String.format("Pulling partition %s from offset %d to %d, range=%d", this.getCurrentPartition(),
          this.nextWatermark.get(this.currentPartitionIdx), this.highWatermark.get(this.currentPartitionIdx),
          this.highWatermark.get(this.currentPartitionIdx) - this.nextWatermark.get(this.currentPartitionIdx)));
      switchMetricContextToCurrentPartition();
    }
    this.stopwatch.start();
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
      this.decodingErrorCount.put(getCurrentPartition(), 1);
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

    // Add error partition count and error message count to workUnitState
    this.workUnitState.setProp(ConfigurationKeys.ERROR_PARTITION_COUNT, this.errorPartitions.size());
    this.workUnitState.setProp(ConfigurationKeys.ERROR_MESSAGE_UNDECODABLE_COUNT, this.undecodableMessageCount);

    // Commit actual high watermark for each partition
    for (int i = 0; i < this.partitions.size(); i++) {
      LOG.info(String.format("Actual high watermark for partition %s=%d, expected=%d", this.partitions.get(i),
          this.nextWatermark.get(i), this.highWatermark.get(i)));
    }
    this.workUnitState.setActualHighWatermark(this.nextWatermark);

    // Commit avg time to pull a record for each partition
    for (KafkaPartition partition : this.partitions) {
      if (this.avgMillisPerRecord.containsKey(partition)) {
        double avgMillis = this.avgMillisPerRecord.get(partition);
        LOG.info(String.format("Avg time to pull a record for partition %s = %f milliseconds", partition, avgMillis));
        KafkaUtils.setPartitionAvgRecordMillis(this.workUnitState, partition, avgMillis);
      } else {
        LOG.info(String.format("Avg time to pull a record for partition %s not recorded", partition));
      }
    }
    this.closer.close();
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }
}
