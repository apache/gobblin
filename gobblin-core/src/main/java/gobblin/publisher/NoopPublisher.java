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

package gobblin.publisher;

import java.io.IOException;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * An no-op optimistic publisher that just inspects the state of all workunits
 * that are successful and marks them committed.
 */
@Slf4j
public class NoopPublisher extends DataPublisher {
  /**
   * @deprecated {@link DataPublisher} initialization should be done in the constructor.
   */
  @Override
  public void initialize()
      throws IOException {

  }

  public NoopPublisher(State state) {
    super(state);
  }

  /**
   * Publish the data for the given tasks.

   * @param states
   */
  @Override
  public void publishData(Collection<? extends WorkUnitState> states)
      throws IOException {
    for (WorkUnitState state: states)
    {
      if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL)
      {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        log.info("Marking state committed");
      }
    }
  }

  /**
   * Publish the metadata (e.g., schema) for the given tasks. Checkpoints should not be published as part of metadata.
   * They are published by Gobblin runtime after the metadata and data are published.

   * @param states
   */
  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states)
      throws IOException {

  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close()
      throws IOException {

  }
}
