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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndOffset;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.gson.Gson;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.Tag;
import gobblin.metrics.kafka.SchemaNotFoundException;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.EventBasedExtractor;


/**
 * An implementation of {@link Extractor} from Apache Kafka. Each extractor processes
 * one or more partitions of the same topic.
 *
 * @author ziliu
 */
public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExtractor.class);

  protected static final Gson GSON = new Gson();
  protected static final Integer MAX_LOG_DECODING_ERRORS = 5;

  protected final WorkUnitState workUnitState;
  protected final String topicName;
  protected final List<KafkaPartition> partitions;
  protected final MultiLongWatermark lowWatermark;
  protected final MultiLongWatermark highWatermark;
  protected final MultiLongWatermark nextWatermark;
  protected final Closer closer;
  protected final KafkaWrapper kafkaWrapper;

  protected final Map<KafkaPartition, Integer> decodingErrorCount;
  protected final Map<KafkaPartition, Long> totalEventSizes;
  protected final Map<KafkaPartition, Integer> eventCounts;
  protected final Map<KafkaPartition, Long> avgEventSizes;

  protected Iterator<MessageAndOffset> messageIterator;
  protected int currentPartitionIdx;

  public KafkaExtractor(WorkUnitState state) {
    super(state);
    this.workUnitState = state;
    this.topicName = KafkaUtils.getTopicName(state);
    this.partitions = KafkaUtils.getPartitions(state);
    this.lowWatermark = GSON.fromJson(state.getWorkunit().getLowWatermark(), MultiLongWatermark.class);
    this.highWatermark = GSON.fromJson(state.getWorkunit().getExpectedHighWatermark(), MultiLongWatermark.class);
    this.nextWatermark = new MultiLongWatermark(this.lowWatermark);
    this.closer = Closer.create();
    this.kafkaWrapper = closer.register(KafkaWrapper.create(state));

    this.decodingErrorCount = Maps.newHashMap();
    this.totalEventSizes = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.eventCounts = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.avgEventSizes = Maps.newHashMapWithExpectedSize(this.partitions.size());

    this.messageIterator = null;
    this.currentPartitionIdx = 0;

    switchMetricContextToCurrentPartition();

    // The actual high watermark starts with the low watermark
    this.workUnitState.setActualHighWatermark(this.lowWatermark);
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<String>("kafkaTopic", KafkaUtils.getTopicName(state)));
    return tags;
  }

  /**
   * Return the next decodable event from the current partition. If the current partition has no more
   * decodable event, move on to the next partition. If all partitions have been processed, return null.
   */
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

        MessageAndOffset nextValidMessage = this.messageIterator.next();

        // Even though we ask Kafka to give us a message buffer starting from offset x, it may
        // return a buffer that starts from offset smaller than x, so we need to skip messages
        // until we get to x.
        if (nextValidMessage.offset() < this.nextWatermark.get(this.currentPartitionIdx)) {
          continue;
        }

        this.nextWatermark.set(this.currentPartitionIdx, nextValidMessage.nextOffset());
        try {
          D record = decodeRecord(nextValidMessage);
          this.maintainStats(nextValidMessage);
          return record;
        } catch (SchemaNotFoundException e) {
          if (shouldLogError()) {
            LOG.error(
                String.format("An event from partition %s has a schema ID that doesn't exist in the schema registry.",
                    getCurrentPartition()),
                e);
            incrementErrorCount();
          }
        } catch (Exception e) {
          if (shouldLogError()) {
            LOG.error(String.format("An event from partition %s cannot be decoded.", getCurrentPartition()), e);
            incrementErrorCount();
          }
        }
      }
    }
    return null;
  }

  private boolean allPartitionsFinished() {
    return this.currentPartitionIdx >= this.highWatermark.size();
  }

  private boolean currentPartitionFinished() {
    return this.nextWatermark.get(this.currentPartitionIdx) >= this.highWatermark.get(this.currentPartitionIdx);
  }

  private void moveToNextPartition() {
    this.currentPartitionIdx++;
    this.messageIterator = null;
    if (this.currentPartitionIdx < this.partitions.size()) {
      switchMetricContextToCurrentPartition();
    }
  }

  private void switchMetricContextToCurrentPartition() {
    if (this.currentPartitionIdx >= this.partitions.size()) {
      return;
    }
    int currentPartitionId = this.getCurrentPartition().getId();
    switchMetricContext(Lists.<Tag<?>> newArrayList(new Tag<Integer>("kafka_partition", currentPartitionId)));
  }

  private Iterator<MessageAndOffset> fetchNextMessageBuffer() {
    return this.kafkaWrapper.fetchNextMessageBuffer(this.partitions.get(this.currentPartitionIdx),
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

  /**
   * Given the message/event just pulled, maintain the average event size of the partition.
   */
  private void maintainStats(MessageAndOffset messageJustPulled) {
    increaseTotalEventSize(getCurrentPartition(), messageJustPulled.message().payloadSize());
    incrementEventCount(getCurrentPartition());
    this.avgEventSizes.put(getCurrentPartition(),
        this.totalEventSizes.get(getCurrentPartition()) / this.eventCounts.get(getCurrentPartition()));
  }

  private void increaseTotalEventSize(KafkaPartition partition, long size) {
    if (this.totalEventSizes.containsKey(partition)) {
      this.totalEventSizes.put(partition, this.totalEventSizes.get(partition) + size);
    } else {
      this.totalEventSizes.put(partition, size);
    }
  }

  private void incrementEventCount(KafkaPartition partition) {
    if (this.eventCounts.containsKey(partition)) {
      this.eventCounts.put(partition, this.eventCounts.get(partition) + 1);
    } else {
      this.eventCounts.put(partition, 1);
    }
  }

  protected abstract D decodeRecord(MessageAndOffset messageAndOffset) throws SchemaNotFoundException, IOException;

  @Override
  public long getExpectedRecordCount() {
    return this.lowWatermark.getGap(this.highWatermark);
  }

  @Override
  public void close() throws IOException {
    for (int i = 0; i < this.partitions.size(); i++) {
      LOG.info(String.format("Last offset pulled for partition %s = %d", this.partitions.get(i),
          this.nextWatermark.get(i) - 1));
    }
    this.workUnitState.setActualHighWatermark(this.nextWatermark);

    // Commit avg event size
    for (KafkaPartition partition : this.partitions) {
      if (this.avgEventSizes.containsKey(partition)) {
        long avgSize = this.avgEventSizes.get(partition);
        LOG.info(String.format("Avg event size pulled for partition %s = %d", partition, avgSize));
        KafkaUtils.setPartitionAvgEventSize(this.workUnitState, partition, avgSize);
      } else {
        LOG.info(String.format("Partition %s not pulled", partition));
        long previousAvgRecordSize =
            KafkaUtils.getPartitionAvgEventSize(this.workUnitState, partition, KafkaSource.DEFAULT_AVG_EVENT_SIZE);
        KafkaUtils.setPartitionAvgEventSize(this.workUnitState, partition, previousAvgRecordSize);
      }
    }
    this.closer.close();
  }

  protected static byte[] getBytes(ByteBuffer buf) {
    byte[] bytes = null;
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    return bytes;
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }
}
