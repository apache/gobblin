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

package gobblin.service;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.kafka.client.DecodeableKafkaRecord;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.kafka.client.Kafka08ConsumerClient;
import gobblin.kafka.client.KafkaConsumerRecord;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstanceConsumer;
import gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.CompletedFuture;


public class SimpleKafkaSpecExecutorInstanceConsumer extends SimpleKafkaSpecExecutorInstance
    implements SpecExecutorInstanceConsumer<Spec>, Closeable {

  // Consumer
  protected final GobblinKafkaConsumerClient _kafka08Consumer;
  protected final List<KafkaPartition> _partitions;
  protected final List<Long> _lowWatermark;
  protected final List<Long> _nextWatermark;
  protected final List<Long> _highWatermark;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private int currentPartitionIdx = -1;
  private boolean isFirstRun = true;

  public SimpleKafkaSpecExecutorInstanceConsumer(Config config, Optional<Logger> log) {
    super(config, log);

    // Consumer
    _kafka08Consumer = new Kafka08ConsumerClient.Factory().create(config);
    List<KafkaTopic> kafkaTopics = _kafka08Consumer.getFilteredTopics(Collections.EMPTY_LIST,
        Lists.newArrayList(Pattern.compile(config.getString(SPEC_KAFKA_TOPICS_KEY))));
    _partitions = kafkaTopics.get(0).getPartitions();
    _lowWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));
    _nextWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));
    _highWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));
  }

  public SimpleKafkaSpecExecutorInstanceConsumer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public SimpleKafkaSpecExecutorInstanceConsumer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  @Override
  public Future<? extends List<Pair<Verb, Spec>>> changedSpecs() {
    List<Pair<Verb, Spec>> changesSpecs = new ArrayList<>();
    initializeWatermarks();
    this.currentPartitionIdx = -1;
    while (!allPartitionsFinished()) {
      if (currentPartitionFinished()) {
        moveToNextPartition();
        continue;
      }
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        try {
          this.messageIterator = fetchNextMessageBuffer();
        } catch (Exception e) {
          _log.error(String.format("Failed to fetch next message buffer for partition %s. Will skip this partition.",
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
        if (nextValidMessage.getOffset() < _nextWatermark.get(this.currentPartitionIdx)) {
          continue;
        }

        _nextWatermark.set(this.currentPartitionIdx, nextValidMessage.getNextOffset());
        try {
          final SpecExecutorInstanceDataPacket record;
          if (nextValidMessage instanceof ByteArrayBasedKafkaRecord) {
            record = decodeRecord((ByteArrayBasedKafkaRecord)nextValidMessage);
          } else if (nextValidMessage instanceof DecodeableKafkaRecord){
            record = ((DecodeableKafkaRecord<?, SpecExecutorInstanceDataPacket>) nextValidMessage).getValue();
          } else {
            throw new IllegalStateException(
                "Unsupported KafkaConsumerRecord type. The returned record can either be ByteArrayBasedKafkaRecord"
                    + " or DecodeableKafkaRecord");
          }
          if (null == record._spec) {
            changesSpecs.add(new ImmutablePair<Verb, Spec>(
              record._verb, new Spec() {
              @Override
              public URI getUri() {
                return record._uri;
              }

              @Override
              public String getVersion() {
                throw new UnsupportedOperationException();
              }

              @Override
              public String getDescription() {
                throw new UnsupportedOperationException();
              }
            }));
          } else {
            changesSpecs.add(new ImmutablePair<Verb, Spec>(record._verb, record._spec));
          }
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    }

    return new CompletedFuture(changesSpecs, null);
  }

  private void initializeWatermarks() {
    initializeLowWatermarks();
    initializeHighWatermarks();
  }

  private void initializeLowWatermarks() {
    try {
      int i=0;
      for (KafkaPartition kafkaPartition : _partitions) {
        if (isFirstRun) {
          long earliestOffset = _kafka08Consumer.getEarliestOffset(kafkaPartition);
          _lowWatermark.set(i, earliestOffset);
        } else {
          _lowWatermark.set(i, _highWatermark.get(i));
        }
        i++;
      }
      isFirstRun = false;
    } catch (KafkaOffsetRetrievalFailureException e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeHighWatermarks() {
    try {
      int i=0;
      for (KafkaPartition kafkaPartition : _partitions) {
        long latestOffset = _kafka08Consumer.getLatestOffset(kafkaPartition);
        _highWatermark.set(i, latestOffset);
        i++;
      }
    } catch (KafkaOffsetRetrievalFailureException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean allPartitionsFinished() {
    return this.currentPartitionIdx >= _nextWatermark.size();
  }

  private boolean currentPartitionFinished() {
    if (this.currentPartitionIdx == -1) {
      return true;
    } else if (_nextWatermark.get(this.currentPartitionIdx) >= _highWatermark.get(this.currentPartitionIdx)) {
      return true;
    } else {
      return false;
    }
  }

  private int moveToNextPartition() {
    this.messageIterator = null;
    return this.currentPartitionIdx ++;
  }

  private KafkaPartition getCurrentPartition() {
    return _partitions.get(this.currentPartitionIdx);
  }

  private Iterator<KafkaConsumerRecord> fetchNextMessageBuffer() {
    return _kafka08Consumer.consume(_partitions.get(this.currentPartitionIdx),
        _nextWatermark.get(this.currentPartitionIdx), _highWatermark.get(this.currentPartitionIdx));
  }

  private SpecExecutorInstanceDataPacket decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
    return SerializationUtils.deserialize(kafkaConsumerRecord.getMessageBytes());
  }

  @Override
  public void close() throws IOException {
    _kafka08Consumer.close();
  }
}