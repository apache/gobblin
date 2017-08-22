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
package org.apache.gobblin.writer;

import org.apache.gobblin.ack.Ackable;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;


/**
 * An envelope for record, watermark pairs that need to be acknowledged.
 * @param <D>: the type of the record
 */
@Alpha
public class AcknowledgableRecordEnvelope<D> implements Ackable {
    private D _record;
    private final AcknowledgableWatermark _watermark;

    public AcknowledgableRecordEnvelope(D record, AcknowledgableWatermark watermark) {
      _record = record;
      _watermark = watermark;
    }

  public D getRecord() {
    return _record;
  }

  /**
   * Create a derived record envelope from this one.
   * Derived envelopes share the same watermark.
   * The original envelope must be acknowledged separately.
   */
  public AcknowledgableRecordEnvelope derivedEnvelope(D record) {
    _watermark.incrementAck();
    return new AcknowledgableRecordEnvelope(record, _watermark);
  }

  @Override
  public void ack() {
    _watermark.ack();
  }

  /**
   * Get the original watermark that was attached to this record,
   * typically by a {@link org.apache.gobblin.source.extractor.StreamingExtractor}
   */
  public CheckpointableWatermark getWatermark() {
    return _watermark.getCheckpointableWatermark();
  }

}
