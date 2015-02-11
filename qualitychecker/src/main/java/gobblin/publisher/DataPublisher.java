/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.publisher;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Defines how to publish data and its corresponding metadata.
 * Can be used for either task level or job level publishing.
 */
public abstract class DataPublisher implements Closeable {

  protected final State state;

  public DataPublisher(State state) {
    this.state = state;
  }

  public abstract void initialize()
      throws IOException;

  /**
   * Returns true if it successfully publishes the data,
   * false otherwise
   */
  public abstract void publishData(Collection<? extends WorkUnitState> tasks)
      throws IOException;

  /**
   * Returns true if it successfully publishes the metadata,
   * false otherwise. Examples are checkpoint files, offsets, etc.
   */
  public abstract void publishMetadata(Collection<? extends WorkUnitState> tasks)
      throws IOException;

  /**
   * Publish the data.
   *
   * @param states task states
   * @throws IOException
   */
  public void publish(Collection<? extends WorkUnitState> states)
      throws IOException {
    publishMetadata(states);
    publishData(states);
  }

  public State getState() {
    return state;
  }
}
