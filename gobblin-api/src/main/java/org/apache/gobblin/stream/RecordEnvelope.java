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

package org.apache.gobblin.stream;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.fork.CopyHelper;
import org.apache.gobblin.fork.CopyNotSupportedException;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;

import javax.annotation.Nullable;


/**
 * An envelope around a record containing metadata and allowing for ack'ing the record.
 *
 * Note:
 * When transforming or cloning a record, it is important to do it in the correct way to ensure callbacks and watermarks
 * are spread correctly:
 *
 * 1-to-1 record transformation:
 *   record.withRecord(transformedRecord);
 *
 * 1-to-n record transformation:
 *   ForkRecordBuilder forkRecordBuilder = record.forkRecordBuilder();
 *   forkRecordBuilder.childRecord(transformed1);
 *   forkRecordBuilder.childRecord(transformed2);
 *   forkRecordBuilder.close();
 *
 * Cloning record:
 *   ForkCloner forkCloner = record.forkCloner();
 *   forkCloner.getClone();
 *   forkCloner.close();
 */
@Alpha
public class RecordEnvelope<D> extends StreamEntity<D> {

  private final D _record;
  @Nullable
  private final CheckpointableWatermark _watermark;

  public RecordEnvelope(D record) {
    this(record, (CheckpointableWatermark) null);
  }

  private RecordEnvelope(D record, RecordEnvelope<?> parentRecord, boolean copyCallbacks) {
    super(parentRecord, copyCallbacks);
    _record = record;
    _watermark = parentRecord._watermark;
  }

  private RecordEnvelope(D record, RecordEnvelope<?>.ForkRecordBuilder<D> forkRecordBuilder, boolean copyCallbacks) {
    super(forkRecordBuilder, copyCallbacks);
    _record = record;
    _watermark = forkRecordBuilder.getRecordEnvelope()._watermark;
  }

  public RecordEnvelope(D record, CheckpointableWatermark watermark) {
    super();
    if (record instanceof RecordEnvelope) {
      throw new IllegalStateException("Cannot wrap a RecordEnvelope in another RecordEnvelope.");
    }

    _record = record;
    _watermark = watermark;
  }

  /**
   * @return a new {@link RecordEnvelope} with just the record changed.
   */
  public <DO> RecordEnvelope<DO> withRecord(DO newRecord) {
    return new RecordEnvelope<>(newRecord, this, true);
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
  protected StreamEntity<D> buildClone() {
    try {
      return new RecordEnvelope<>((D) CopyHelper.copy(_record), this, false);
    } catch (CopyNotSupportedException cnse) {
      throw new UnsupportedOperationException(cnse);
    }
  }

  /**
   * Obtain a {@link ForkRecordBuilder} to create derivative records to this record.
   */
  public <DO> ForkRecordBuilder<DO> forkRecordBuilder() {
    return new ForkRecordBuilder<>();
  }

  /**
   * Used to create derivative records with the same callbacks and watermarks.
   */
  public class ForkRecordBuilder<DO> extends StreamEntity.ForkedEntityBuilder {
    private ForkRecordBuilder() {
    }

    /**
     * Create a new child {@link RecordEnvelope} with the specified record.
     */
    public RecordEnvelope<DO> childRecord(DO newRecord) {
      return new RecordEnvelope<>(newRecord, this, true);
    }

    RecordEnvelope<D> getRecordEnvelope() {
      return RecordEnvelope.this;
    }
  }
}
