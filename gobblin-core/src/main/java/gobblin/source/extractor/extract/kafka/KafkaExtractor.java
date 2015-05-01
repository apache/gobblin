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
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.EventBasedExtractor;


/**
 * An implementation of {@link Extractor} from Apache Kafka.
 *
 * @param <K> Type of Kafka event key. When using the Kafka Old API, K should be ByteBuffer.
 * @param <V> Type of Kafka event value. When using the Kafka Old API, V should be ByteBuffer.
 *
 * @author ziliu
 */
public abstract class KafkaExtractor<S, K, V, D> extends EventBasedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExtractor.class);

  protected final WorkUnitState workUnitState;
  protected final KafkaPartition partition;
  protected final long lowWatermark;
  protected final long highWatermark;
  protected final Closer closer;
  protected final KafkaWrapper<K, V> kafkaWrapper;

  protected Iterator<KafkaEvent<K, V>> messageIterator;
  protected long nextWatermark;

  private long totalRecordSize;
  private int recordCount;
  private long avgRecordSize;

  public KafkaExtractor(WorkUnitState state) {
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
    this.kafkaWrapper = closer.register(new KafkaWrapper<K, V>(state));
    this.messageIterator = null;
    this.nextWatermark = this.lowWatermark;
    this.totalRecordSize = 0;
    this.recordCount = 0;
    this.avgRecordSize = 0;
  }

  @Override
  public D readRecord(D reuse) throws DataRecordException, IOException {
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

    KafkaEvent<K, V> nextValidMessage = null;
    do {
      if (!this.messageIterator.hasNext()) {
        return null;
      }
      nextValidMessage = this.messageIterator.next();
    } while (nextValidMessage.offset() < this.nextWatermark);

    this.nextWatermark = nextValidMessage.offset() + 1;
    try {
      D record = decodeRecord(nextValidMessage);
      this.maintainStats(nextValidMessage);
      return record;
    } catch (SchemaNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void maintainStats(KafkaEvent<K, V> nextValidMessage) {
    this.totalRecordSize += getEventSize(nextValidMessage);
    this.recordCount++;
    this.avgRecordSize = this.totalRecordSize / this.recordCount;
  }

  /**
   * Get the size of the value of the event in bytes.
   */
  protected abstract long getEventSize(KafkaEvent<K, V> event);

  /**
   * Decode the value of the event from a V object into a D object.
   */
  protected abstract D decodeRecord(KafkaEvent<K, V> event) throws SchemaNotFoundException, IOException;

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
}
