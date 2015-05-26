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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndOffset;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.Tag;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.EventBasedExtractor;


/**
 * An implementation of {@link Extractor} from Apache Kafka.
 *
 * @author ziliu
 */
public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExtractor.class);

  protected final WorkUnitState workUnitState;
  protected final KafkaPartition partition;
  protected final long lowWatermark;
  protected final long highWatermark;
  protected final Closer closer;
  protected final KafkaWrapper kafkaWrapper;

  protected Iterator<MessageAndOffset> messageIterator;
  protected long nextWatermark;
  private long totalRecordSize;
  private int recordCount;
  private long avgRecordSize;

  public KafkaExtractor(WorkUnitState state) {
    super(state);
    this.workUnitState = state;
    this.partition =
        new KafkaPartition.Builder().withId(state.getPropAsInt(KafkaSource.PARTITION_ID))
            .withTopicName(state.getProp(KafkaSource.TOPIC_NAME))
            .withLeaderId(state.getPropAsInt(KafkaSource.LEADER_ID))
            .withLeaderHost(state.getProp(KafkaSource.LEADER_HOST))
            .withLeaderPort(state.getPropAsInt(KafkaSource.LEADER_PORT)).build();
    this.lowWatermark = state.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    this.highWatermark = state.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
    this.closer = Closer.create();
    this.kafkaWrapper = closer.register(KafkaWrapper.create(state));
    this.messageIterator = null;
    this.nextWatermark = this.lowWatermark;
    this.totalRecordSize = 0;
    this.recordCount = 0;
    this.avgRecordSize = 0;

    switchMetricContext(Lists.<Tag<?>>newArrayList(new Tag<Integer>("partition", this.partition.getId())));
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<String>("topic", state.getProp(KafkaSource.TOPIC_NAME)));
    return tags;
  }

  @Override
  public D readRecordImpl(D reuse) throws DataRecordException, IOException {
    if (this.nextWatermark >= this.highWatermark) {
      return null;
    }
    if (this.messageIterator == null || !this.messageIterator.hasNext()) {
      this.messageIterator =
          this.kafkaWrapper.fetchNextMessageBuffer(this.partition, this.nextWatermark, this.highWatermark);
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        return null;
      }
    }

    MessageAndOffset nextValidMessage = null;
    do {
      if (!this.messageIterator.hasNext()) {
        return null;
      }
      nextValidMessage = this.messageIterator.next();
    } while (nextValidMessage.offset() < this.nextWatermark);

    this.nextWatermark = nextValidMessage.offset() + 1;
    try {
      D record = decodeRecord(nextValidMessage, null);
      this.maintainStats(nextValidMessage);
      return record;
    } catch (SchemaNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void maintainStats(MessageAndOffset nextValidMessage) {
    this.totalRecordSize += nextValidMessage.message().payloadSize();
    this.recordCount++;
    this.avgRecordSize = this.totalRecordSize / this.recordCount;
  }

  protected abstract D decodeRecord(MessageAndOffset messageAndOffset, D reuse) throws SchemaNotFoundException,
      IOException;

  @Override
  public long getExpectedRecordCount() {
    return this.highWatermark - this.lowWatermark;
  }

  @Override
  public long getHighWatermark() {
    return this.highWatermark;
  }

  @Override
  public void close() throws IOException {

    // Commit high watermark
    LOG.info(String.format("Last offset pulled for partition %s:%d = %d", this.partition.getTopicName(),
        this.partition.getId(), this.nextWatermark - 1));
    this.workUnitState.setHighWaterMark(this.nextWatermark);

    // Commit avg event size
    if (this.avgRecordSize > 0) {
      LOG.info(String.format("Avg event size pulled for partition %s:%d = %d", this.partition.getTopicName(),
          this.partition.getId(), this.avgRecordSize));
      this.workUnitState.setProp(KafkaSource.getWorkUnitSizePropName(this.workUnitState), this.avgRecordSize);
    } else {
      LOG.info(String.format("Partition %s:%d not pulled", this.partition.getTopicName(), this.partition.getId()));
      long previousAvgRecordSize =
          this.workUnitState.getPropAsLong(KafkaSource.getWorkUnitSizePropName(this.workUnitState),
              KafkaSource.DEFAULT_AVG_EVENT_SIZE);
      this.workUnitState.setProp(KafkaSource.getWorkUnitSizePropName(this.workUnitState), previousAvgRecordSize);
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
}
