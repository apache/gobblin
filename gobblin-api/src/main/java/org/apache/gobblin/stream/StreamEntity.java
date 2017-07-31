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

import java.io.Closeable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.gobblin.ack.Ackable;
import org.apache.gobblin.ack.HierarchicalAckable;


/**
 * An entity in the Gobblin ingestion stream.
 *
 * Note:
 * When transforming or cloning a stream entity, it is important to construct the new entity in the correct way to ensure callbacks
 * are spread correctly:
 *
 * 1-to-1 entity transformation constructor:
 *   super(upstreamEntity, true);
 *
 * 1-to-n entity transformation:
 *   ForkedEntityBuilder forkedEntityBuilder = new ForkedEntityBuilder();
 *
 *   super(forkedEntityBuilder, true);
 *
 *   forkedEntityBuilder.close();
 *
 * Cloning entity:
 *   ForkCloner forkCloner = record.forkCloner();
 *   forkCloner.getClone();
 *   forkCloner.close();
 *
 * @param <D> the type of records represented in the stream.
 */
public abstract class StreamEntity<D> implements Ackable {

  private final List<Ackable> _callbacks;
  private boolean _callbacksUsedForDerivedEntity = false;

  protected StreamEntity() {
    _callbacks = Lists.newArrayList();
  }

  protected StreamEntity(StreamEntity<?> upstreamEntity, boolean copyCallbacks) {
    if (copyCallbacks) {
      _callbacks = upstreamEntity.getCallbacksForDerivedEntity();
    } else {
      _callbacks = Lists.newArrayList();
    }
  }

  protected StreamEntity(ForkedEntityBuilder forkedEntityBuilder, boolean copyCallbacks) {
    if (copyCallbacks) {
      _callbacks = forkedEntityBuilder.getChildCallback();
    } else {
      _callbacks = Lists.newArrayList();
    }
  }

  @Override
  public void ack() {
    for (Ackable ackable : _callbacks) {
      ackable.ack();
    }
  }

  @Override
  public void nack(Throwable error) {
    for (Ackable ackable : _callbacks) {
      ackable.nack(error);
    }
  }

  private synchronized List<Ackable> getCallbacksForDerivedEntity() {
    Preconditions.checkState(!_callbacksUsedForDerivedEntity,
        "StreamEntity was attempted to use more than once for a derived entity.");
    _callbacksUsedForDerivedEntity = true;
    return _callbacks;
  }

  public StreamEntity<D> addCallBack(Ackable ackable) {
    _callbacks.add(ackable);
    return this;
  }

  private void setCallbacks(List<Ackable> callbacks) {
    _callbacks.addAll(callbacks);
  }

  /**
   * @return a clone of this {@link StreamEntity}.
   */
  public final StreamEntity<D> getSingleClone() {
    StreamEntity<D> entity = buildClone();
    entity.setCallbacks(getCallbacksForDerivedEntity());
    return entity;
  }

  /**
   * @return a clone of this {@link StreamEntity}. Implementations need not worry about the callbacks, they will be set
   * automatically.
   */
  protected abstract StreamEntity<D> buildClone();

  /**
   * @return a {@link ForkCloner} to generate multiple clones of this {@link StreamEntity}.
   */
  public ForkCloner forkCloner() {
    return new ForkCloner();
  }

  /**
   * Used to clone a {@link StreamEntity} retaining the correct callbacks.
   */
  public class ForkCloner implements Closeable {

    private final ForkedEntityBuilder _forkedEntityBuilder = new ForkedEntityBuilder();

    private ForkCloner() {
    }

    public StreamEntity<D> getClone() {
      StreamEntity<D> entity = buildClone();
      entity.setCallbacks(_forkedEntityBuilder.getChildCallback());
      return entity;
    }

    @Override
    public void close() {
      _forkedEntityBuilder.close();
    }
  }

  /**
   * Used to generate derived {@link StreamEntity}s using a {@link HierarchicalAckable} to make sure parent ackable
   * is automatically acked when all children are acked.
   */
  public class ForkedEntityBuilder implements Closeable {
    private final HierarchicalAckable _hierarchicalAckable;

    protected ForkedEntityBuilder() {
      List<Ackable> callbacks = getCallbacksForDerivedEntity();
      _hierarchicalAckable = callbacks.isEmpty() ? null : new HierarchicalAckable(callbacks);
    }

    protected List<Ackable> getChildCallback() {
      return _hierarchicalAckable == null ? Lists.newArrayList() : Lists.newArrayList(_hierarchicalAckable.newChildAckable());
    }

    @Override
    public void close() {
      if (_hierarchicalAckable != null) {
        _hierarchicalAckable.close();
      }
    }
  }

}
