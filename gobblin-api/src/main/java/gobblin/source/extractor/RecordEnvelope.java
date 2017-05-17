/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.source.extractor;

import java.io.Closeable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import gobblin.metadata.MetadataNames;
import gobblin.metadata.types.Metadata;
import gobblin.writer.Ackable;


/**
 * A wrapper around a record. This is the unit that traverses a Gobblin ingestion task.
 * @param <D> type of the record contained.
 */
@Alpha
public class RecordEnvelope<D> implements Ackable {

  private final D _record;
  private final Metadata _metadata;
  private final List<Callback> _callbacks;

  private volatile boolean _acked = false;
  private volatile boolean _descendantBuilt = false;

  public RecordEnvelope(D record) {
    this(record, new Metadata(), Lists.<Callback>newArrayList());
  }

  private RecordEnvelope(D record, Metadata metadata, List<Callback> callbacks) {
    if (record instanceof RecordEnvelope) {
      throw new IllegalStateException("Cannot envelope and already enveloped record.");
    }
    Preconditions.checkArgument(recordCanBeNull() || record != null, "Record cannot be null.");
    this._record = record;
    this._metadata = metadata;
    this._callbacks = callbacks;
    this._metadata.getRecordMetadata().put(MetadataNames.RECORD_PROCESSING_START_NANOS, System.nanoTime());
  }

  /**
   * Create a {@link RecordEnvelope} with the same metadata and callbacks as this one, but a different record.
   * This method is used when a record is transformed.
   *
   * Note: {@link #withRecord(Object)} and {@link #forkedRecordBuilder()} can only be called once, and only one of them
   * can be called. This is necessary to handle callbacks correctly.
   */
  public synchronized <DO> RecordEnvelope<DO> withRecord(DO record) {
    if (_descendantBuilt) {
      throw new IllegalStateException("A descendant record has already been built.");
    }
    _descendantBuilt = true;
    return new RecordEnvelope<>(record, _metadata, _callbacks);
  }

  /**
   * Used to create {@link RecordEnvelope} with the same metadata and a forked callback as this one. This method
   * is used when a record is forked, either in a {@link gobblin.fork.ForkOperator} or in 1 to N
   * {@link gobblin.converter.Converter}s.
   *
   * Note: {@link #withRecord(Object)} and {@link #forkedRecordBuilder()} can only be called once, and only one of them
   * can be called. This is necessary to handle callbacks correctly.
   *
   * @return a {@link ForkedRecordBuilder}.
   */
  public synchronized ForkedRecordBuilder forkedRecordBuilder() {
    if (_descendantBuilt) {
      throw new IllegalStateException("A descendant record has already been built.");
    }
    _descendantBuilt = true;
    return new ForkedRecordBuilder();
  }

  /**
   * Used to create {@link RecordEnvelope} with the same metadata and a forked callback as this one. This class
   * is used when a record is forked, either in a {@link gobblin.fork.ForkOperator} or in 1 to N
   * {@link gobblin.converter.Converter}s.
   *
   * Users should call {@link #forkWithRecord(Object)} to generate the {@link RecordEnvelope} for each forked record,
   * and must call {@link #close()} after all forked records have been created.
   */
  public class ForkedRecordBuilder implements Closeable {

    private final HierarchicalCallback _hierarchicalCallback = new HierarchicalCallback(RecordEnvelope.this);
    private final boolean _anyCallbacks = _callbacks.size() > 0;

    private ForkedRecordBuilder() {
    }

    /**
     * Create a {@link RecordEnvelope} with the same metadata and a forked callback.
     */
    public <DO> RecordEnvelope<DO> forkWithRecord(DO record) {
      List<Callback> newCallbacks = _anyCallbacks ? Lists.newArrayList(_hierarchicalCallback.newChildCallback()) :
          Lists.newArrayList();
      return new RecordEnvelope<>(record, _metadata, newCallbacks);
    }

    /**
     * Indicates that no more records will be forked. Note that if this method is not called, the parent callback will
     * never be called (as the framework does not know whether new forked records from the same input record are still expected).
     */
    @Override
    public void close() {
      _hierarchicalCallback.close();
    }
  }

  /**
   * @return the record inside this envelope.
   */
  public D getRecord() {
    return _record;
  }

  /**
   * @return the metadata for this record.
   */
  public Metadata getMetadata() {
    return _metadata;
  }

  /**
   * Add a success-only callback to this envelope.
   */
  public void addSuccessCallback(Runnable callback) {
    _callbacks.add(new Callback.SuccessCallback(callback));
  }

  /**
   * Add a failure-only callback to this envelope.
   */
  public void addFailureCallback(Runnable callback) {
    _callbacks.add(new Callback.FailureCallback(callback));
  }

  /**
   * Add a callback to this envelope.
   */
  public void addCallback(Callback callback) {
    _callbacks.add(callback);
  }

  /**
   * Acknowledge that processing for this record has completed. This method should only be called when the record is flushed
   * to the output, or when the record is dropped from the flow.
   *
   * The method will trigger all callbacks registered.
   */
  @Override
  public synchronized void ack() {
    if (_acked) {
      return;
    }
    _acked = true;
    for (Callback callback : _callbacks) {
      callback.onSuccess();
    }
  }

  /**
   * Negative acknowledgement. Indicates that this record failed to be fully processed by the pipeline.
   *
   * The method will trigger all callbacks registered.
   */
  @Override
  public synchronized void nack(Throwable cause) {
    if (_acked) {
      return;
    }
    _acked = true;
    for (Callback callback : _callbacks) {
      callback.onFailure(cause);
    }
  }

  protected boolean recordCanBeNull() {
    return false;
  }
}
