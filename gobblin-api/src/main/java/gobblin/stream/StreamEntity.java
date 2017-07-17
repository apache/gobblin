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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.writer.Ackable;
import gobblin.writer.HierarchicalAckable;


/**
 * An entity in the Gobblin ingestion stream.
 * @param <D> the type of records represented in the stream.
 */
public abstract class StreamEntity<D> implements Ackable {

  private final List<Ackable> _callbacks;
  private boolean _callbacksUsedForDerivedEntity = false;

  protected StreamEntity() {
    _callbacks = Lists.newArrayList();
  }

  protected StreamEntity(StreamEntity<?> upstreamEntity) {
    _callbacks = upstreamEntity.getCallbacksForDerivedEntity();
  }

  protected StreamEntity(ForkedEntityBuilder forkedEntityBuilder) {
    _callbacks = forkedEntityBuilder.getChildCallback();
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

  public void addCallBack(Ackable ackable) {
    _callbacks.add(ackable);
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

  public ForkCloner forkCloner() {
    return new ForkCloner();
  }

  public ForkedEntityBuilder forkedEntityBuilder() {
    return new ForkedEntityBuilder();
  }

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
    public void close() throws IOException {
      _forkedEntityBuilder.close();
    }
  }

  public class ForkedEntityBuilder implements Closeable {
    private final HierarchicalAckable _hierarchicalAckable;

    public ForkedEntityBuilder() {
      List<Ackable> callbacks = getCallbacksForDerivedEntity();
      _hierarchicalAckable = callbacks.isEmpty() ? null : new HierarchicalAckable(callbacks);
    }

    private List<Ackable> getChildCallback() {
      return _hierarchicalAckable == null ? Lists.newArrayList() : Lists.newArrayList(_hierarchicalAckable.newChildAckable());
    }

    @Override
    public void close() throws IOException {
      _hierarchicalAckable.close();
    }
  }

}
