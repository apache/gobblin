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

package org.apache.gobblin.metastore.metadata;

import java.io.IOException;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;


import lombok.Data;


/**
 * Contains metadata about an entry in a {@link StateStore}.
 * Exposes access to the {@link StateStore} that contains the entry.
 * @param <T> type of {@link State} that can be read from this entry.
 */
@Data
public abstract class StateStoreEntryManager<T extends State> {

  private final String storeName;
  private final String tableName;
  /** Timestamp at which the state was written. */
  private final long timestamp;

  /** {@link StateStore} where this entry exists. */
  private final StateStore stateStore;

  public final long getTimestamp() {
    if (this.timestamp <= 0) {
      throw new RuntimeException("Timestamp is not reliable.");
    }
    return this.timestamp;
  }

  /**
   * @return The {@link State} contained in this entry. This operation should be lazy.
   * @throws IOException
   */
  public abstract T readState() throws IOException;

  /**
   * Delete this entry in the {@link StateStore}.
   * @throws IOException
   */
  public abstract void delete() throws IOException;

}
