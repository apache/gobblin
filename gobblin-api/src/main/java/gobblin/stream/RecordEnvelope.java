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

package gobblin.stream;

import java.util.function.Function;

import gobblin.annotation.Alpha;
import gobblin.fork.CopyHelper;
import gobblin.fork.CopyNotSupportedException;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.writer.Ackable;

import javax.annotation.Nullable;


/**
 * An envelope around a record containing metadata and allowing for ack'ing the record.
 */
@Alpha
public class RecordEnvelope<D> extends StreamEntity<D> {

  private final D _record;
  private final CheckpointableWatermark _watermark;
  private final Ackable _ackable;

  public RecordEnvelope(D record) {
    this(record, null, null);
  }

  public RecordEnvelope(D record, CheckpointableWatermark watermark) {
    this(record, watermark, null);
  }

  private RecordEnvelope(D record, CheckpointableWatermark watermark, Ackable ackable) {
    super(ackable);

    if (record instanceof RecordEnvelope) {
      throw new IllegalStateException("Cannot wrap a RecordEnvelope in another RecordEnvelope.");
    }
    _record = record;
    _watermark = watermark;
    _ackable = ackable;
  }

  /**
   * @return a new {@link RecordEnvelope} with just the record changed.
   */
  public <DO> RecordEnvelope<DO> withRecord(DO newRecord) {
    return new RecordEnvelope<>(newRecord, _watermark, _ackable);
  }

  /**
   * @return a new {@link RecordEnvelope} with just the record changed using a lambda expression.
   */
  public <DO> RecordEnvelope<DO> mapRecord(Function<D, DO> mapper) {
    return new RecordEnvelope<>(mapper.apply(_record), _watermark, _ackable);
  }

  /**
   * @return a new {@link RecordEnvelope} with an {@link Ackable} that will be triggered upon {@link #ack()}.
   * @deprecated this is a temporary method while streaming is refined
   */
  @Deprecated
  public RecordEnvelope<D> withAckableWatermark(Ackable ackableWatermark) {
    return new RecordEnvelope<>(_record, _watermark, ackableWatermark);
  }

  /**
   * @return the record contained.
   */
  public D getRecord() {
    return _record;
  }

  /**
   * @return The watermark for this record.
   */
  @Nullable public CheckpointableWatermark getWatermark() {
    return _watermark;
  }

  @Override
  public StreamEntity<D> getClone() {
    try {
      return withRecord((D) CopyHelper.copy(_record));
    } catch (CopyNotSupportedException cnse) {
      throw new UnsupportedOperationException(cnse);
    }
  }
}
