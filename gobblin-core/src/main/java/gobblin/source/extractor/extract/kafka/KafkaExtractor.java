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

import kafka.message.MessageAndOffset;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.EventBasedExtractor;


/**
 * An implementation of {@link Extractor} from Apache Kafka.
 *
 * @author ziliu
 */
public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  protected final WorkUnitState workUnitState;
  protected final KafkaPartition partition;
  protected final long lowWatermark;
  protected final long highWatermark;
  protected final Closer closer;
  protected final KafkaWrapper kafkaWrapper;

  protected Iterator<MessageAndOffset> messageIterator;
  protected long nextWatermark;

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
    this.kafkaWrapper = closer.register(KafkaWrapper.create(state));
    this.messageIterator = null;
    this.nextWatermark = this.lowWatermark;
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

    MessageAndOffset nextValidMessage = null;
    do {
      if (!this.messageIterator.hasNext()) {
        return null;
      }
      nextValidMessage = this.messageIterator.next();
    } while (nextValidMessage.offset() < this.nextWatermark);

    this.nextWatermark = nextValidMessage.offset() + 1;
    try {
      return decodeRecord(nextValidMessage, null);
    } catch (SchemaNotFoundException e) {
      throw new RuntimeException(e);
    }
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
    this.workUnitState.setHighWaterMark(this.nextWatermark);
    this.closer.close();
  }

}
