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
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import gobblin.metrics.Tag;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.DataRecordException;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkSerializerHelper;
import gobblin.source.extractor.extract.EventBasedExtractor;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.ConfigUtils;
import gobblin.writer.WatermarkStorage;

import lombok.ToString;


/**
 * An implementation of {@link StreamingExtractor}  which reads from Kafka and returns records . Type of record depends on deserializer set.
 *
 * @author Shrikanth Shankar
 *
 *
 */
public class KafkaSimpleStreamingExtractor<S, D> extends EventBasedExtractor<S, D> implements StreamingExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleStreamingExtractor.class);
  private AtomicBoolean _isStarted = new AtomicBoolean(false);

  @Override
  public void start(WatermarkStorage watermarkStorage)
      throws IOException {
    Preconditions.checkArgument(watermarkStorage != null, "Watermark Storage should not be null");
    Map<String, CheckpointableWatermark> watermarkMap =
        watermarkStorage.getCommittedWatermarks(KafkaWatermark.class, Collections.singletonList(_partition.toString()));
    KafkaWatermark watermark = (KafkaWatermark) watermarkMap.get(_partition.toString());
    if (watermark == null) {
      LOG.info("Offset is null - seeking to beginning of topic and partition for {} ", _partition.toString());
      _consumer.seekToBeginning(_partition);
    } else {
      // seek needs to go one past the last committed offset
      LOG.info("Offset found in consumer for partition {}. Seeking to one past what we found : {}",
          _partition.toString(), watermark.getLwm().getValue() + 1);
      _consumer.seek(_partition, watermark.getLwm().getValue() + 1);
    }
    _isStarted.set(true);
  }

  @ToString
  public static class KafkaWatermark implements CheckpointableWatermark {
    TopicPartition _topicPartition;
    LongWatermark _lwm;

    @VisibleForTesting
    public KafkaWatermark(TopicPartition topicPartition, LongWatermark lwm) {
      _topicPartition = topicPartition;
      _lwm = lwm;
    }

    @Override
    public String getSource() {
      return _topicPartition.toString();
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
      Preconditions.checkArgument(_topicPartition.equals(ko._topicPartition));
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
      return _topicPartition.hashCode() * prime + _lwm.hashCode();
    }

    public TopicPartition getTopicPartition() {
      return _topicPartition;
    }

    public LongWatermark getLwm() {
      return _lwm;
    }
  }

  private final Consumer<S, D> _consumer;
  private final TopicPartition _partition;
  private Iterator<ConsumerRecord<S, D>> _records;
  AtomicLong _rowCount = new AtomicLong(0);
  protected final Optional<KafkaSchemaRegistry<String, S>> _schemaRegistry;
  protected AtomicBoolean _close = new AtomicBoolean(false);
  private final long fetchTimeOut;

  public KafkaSimpleStreamingExtractor(WorkUnitState state) {
    super(state);
    _consumer = KafkaSimpleStreamingSource.getKafkaConsumer(ConfigUtils.propertiesToConfig(state.getProperties()));
    closer.register(_consumer);
    _partition = new TopicPartition(KafkaSimpleStreamingSource.getTopicNameFromState(state),
        KafkaSimpleStreamingSource.getPartitionIdFromState(state));
    _consumer.assign(Collections.singletonList(_partition));
    this._schemaRegistry = state.contains(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS) ? Optional
        .of(KafkaSchemaRegistry.<String, S>get(state.getProperties()))
        : Optional.<KafkaSchemaRegistry<String, S>>absent();
    this.fetchTimeOut = state.getPropAsLong(AbstractBaseKafkaConsumerClient.CONFIG_KAFKA_FETCH_TIMEOUT_VALUE,
        AbstractBaseKafkaConsumerClient.CONFIG_KAFKA_FETCH_TIMEOUT_VALUE_DEFAULT);
  }

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return the schema of Kafka topic being extracted
   * @throws IOException if there is problem getting the schema
   */
  @Override
  public S getSchema()
      throws IOException {
    try {
      if (_schemaRegistry.isPresent()) {
        return _schemaRegistry.get().getLatestSchemaByTopic(this._partition.topic());
      }
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
    return ((S) this._partition.topic());
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<>("kafkaTopic", state.getProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST)));
    return tags;
  }

  /**
   * Return the next record when available. Will never time out since this is a streaming source.
   */
  @Override
  public RecordEnvelope<D> readRecordEnvelopeImpl()
      throws DataRecordException, IOException {
    if (!_isStarted.get()) {
      throw new IOException("Streaming extractor has not been started.");
    }
    while ((_records == null) || (!_records.hasNext())) {
      synchronized (_consumer) {
        if (_close.get()) {
          throw new ClosedChannelException();
        }
        _records = _consumer.poll(this.fetchTimeOut).iterator();
      }
    }
    ConsumerRecord<S, D> record = _records.next();
    _rowCount.getAndIncrement();
    return new RecordEnvelope<D>(record.value(), new KafkaWatermark(_partition, new LongWatermark(record.offset())));
  }

  @Override
  public long getExpectedRecordCount() {
    return _rowCount.get();
  }

  @Override
  public void close()
      throws IOException {
    _close.set(true);
    _consumer.wakeup();
    synchronized (_consumer) {
      closer.close();
    }
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }
}
