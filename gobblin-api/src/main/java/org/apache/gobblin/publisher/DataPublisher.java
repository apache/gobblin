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

package org.apache.gobblin.publisher;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;

import org.apache.gobblin.capability.Capability;
import org.apache.gobblin.capability.CapabilityAware;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * Defines how to publish data and its corresponding metadata. Can be used for either task level or job level publishing.
 */
public abstract class DataPublisher implements Closeable, CapabilityAware {
  /**
   * Reusable capability.
   */
  public static final Capability REUSABLE = new Capability("REUSABLE", false);

  protected final State state;

  public DataPublisher(State state) {
    this.state = state;
  }

  /**
   * @deprecated {@link DataPublisher} initialization should be done in the constructor.
   */
  @Deprecated
  public abstract void initialize()
      throws IOException;

  /**
   * Publish the data for the given tasks.
   */
  public abstract void publishData(Collection<? extends WorkUnitState> states)
      throws IOException;

  /**
   * Publish the metadata (e.g., schema) for the given tasks. Checkpoints should not be published as part of metadata.
   * They are published by Gobblin runtime after the metadata and data are published.
   */
  public abstract void publishMetadata(Collection<? extends WorkUnitState> states)
      throws IOException;

  /**
   * First publish the metadata via {@link DataPublisher#publishMetadata(Collection)}, and then publish the output data
   * via the {@link DataPublisher#publishData(Collection)} method.
   *
   * @param states is a {@link Collection} of {@link WorkUnitState}s.
   * @throws IOException if there is a problem with publishing the metadata or the data.
   */
  public void publish(Collection<? extends WorkUnitState> states)
      throws IOException {
    if (shouldPublishMetadataFirst()) {
      publishMetadata(states);
      publishData(states);
    } else {
      publishData(states);
      publishMetadata(states);
    }
    markCommit(states);
  }

  protected void markCommit(Collection<? extends WorkUnitState> states) {
    for (WorkUnitState workUnitState : states) {
      if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
        workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
    }
  }

  public State getState() {
    return this.state;
  }

  /**
   * Get an instance of {@link DataPublisher}.
   *
   * @param dataPublisherClass A concrete class that extends {@link DataPublisher}.
   * @param state A {@link State} used to instantiate the {@link DataPublisher}.
   * @return A {@link DataPublisher} instance.
   */
  public static DataPublisher getInstance(Class<? extends DataPublisher> dataPublisherClass, State state)
      throws ReflectiveOperationException {
    Constructor<? extends DataPublisher> dataPublisherConstructor = dataPublisherClass.getConstructor(State.class);
    return dataPublisherConstructor.newInstance(state);
  }

  /**
   * Returns true if the implementation of {@link DataPublisher} is thread-safe.
   *
   * <p>
   *   For a thread-safe {@link DataPublisher}, this method should return this.getClass() == <class>.class
   *   to ensure that any extensions must explicitly be marked as thread safe.
   * </p>
   */
  public boolean isThreadSafe() {
    return this.getClass() == DataPublisher.class;
  }

  /**
   * Return true if the current publisher can be skipped.
   *
   * <p>
   *   For a publisher that can be skipped, it should not have any effect on state persistence. It will be skipped when
   *   a job is cancelled, and all finished tasks are configured to be committed.
   * </p>
   */
  public boolean canBeSkipped() {
    return this.state.getPropAsBoolean(ConfigurationKeys.DATA_PUBLISHER_CAN_BE_SKIPPED,
        ConfigurationKeys.DEFAULT_DATA_PUBLISHER_CAN_BE_SKIPPED);
  }

  /**
   * Generally metadata should be published before the data it represents, but this allows subclasses to override
   * if they are dependent on data getting published first.
   */
  protected boolean shouldPublishMetadataFirst() {
    return true;
  }

  @Override
  public boolean supportsCapability(Capability c, Map<String, Object> properties) {
    return false;
  }
}
