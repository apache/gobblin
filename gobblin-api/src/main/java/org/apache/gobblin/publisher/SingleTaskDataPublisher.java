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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * A subclass of {@link DataPublisher} that can publish data of a single {@link Task}.
 */
public abstract class SingleTaskDataPublisher extends DataPublisher {

  public SingleTaskDataPublisher(State state) {
    super(state);
  }

  /**
   * Publish the data for the given task. The state must contains property "writer.final.output.file.paths"
   * (or "writer.final.output.file.paths.[branchId]" if there are multiple branches),
   * so that the publisher knows which files to publish.
   */
  public abstract void publishData(WorkUnitState state) throws IOException;

  /**
   * Publish the metadata (e.g., schema) for the given task. Checkpoints should not be published as part of metadata.
   * They are published by Gobblin runtime after the metadata and data are published.
   */
  public abstract void publishMetadata(WorkUnitState state) throws IOException;

  /**
   * First publish the metadata via {@link DataPublisher#publishMetadata(WorkUnitState)}, and then publish the output data
   * via the {@link DataPublisher#publishData(WorkUnitState)} method.
   *
   * @param state is a {@link WorkUnitState}.
   * @throws IOException if there is a problem with publishing the metadata or the data.
   */
  public void publish(WorkUnitState state) throws IOException {
    publishMetadata(state);
    publishData(state);
  }

  /**
   * Get an instance of {@link SingleTaskDataPublisher}.
   *
   * @param dataPublisherClass A concrete class that extends {@link SingleTaskDataPublisher}.
   * @param state A {@link State} used to instantiate the {@link SingleTaskDataPublisher}.
   * @return A {@link SingleTaskDataPublisher} instance.
   * @throws ReflectiveOperationException
   */
  public static SingleTaskDataPublisher getInstance(Class<? extends DataPublisher> dataPublisherClass, State state)
      throws ReflectiveOperationException {
    Preconditions.checkArgument(SingleTaskDataPublisher.class.isAssignableFrom(dataPublisherClass),
        String.format("Cannot instantiate %s since it does not extend %s", dataPublisherClass.getSimpleName(),
            SingleTaskDataPublisher.class.getSimpleName()));

    return (SingleTaskDataPublisher) DataPublisher.getInstance(dataPublisherClass, state);
  }
}
