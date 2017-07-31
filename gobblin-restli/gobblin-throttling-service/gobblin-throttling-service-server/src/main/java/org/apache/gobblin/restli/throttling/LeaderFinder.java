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

package org.apache.gobblin.restli.throttling;

import java.io.Serializable;

import com.google.common.util.concurrent.Service;


/**
 * An interface to find the leader in a cluster of processes.
 *
 * Multiple processes that instantiate equivalent {@link LeaderFinder}s automatically elect a single leader at all times.
 * Each process can verify whether it is the leader using {@link #isLeader()}, and it can obtain the metadata of the
 * current leader using {@link #getLeaderMetadata()}.
 *
 * The metadata is application specific, but it might include the {@link java.net.URI} of the leader for example.
 *
 * @param <T>
 */
public interface LeaderFinder<T extends LeaderFinder.Metadata> extends Service {

  /**
   * @return true if the current process is the leader.
   */
  boolean isLeader();

  /**
   * @return The metadata of the current leader.
   */
  T getLeaderMetadata();

  /**
   * @return The metadata of the current process.
   */
  T getLocalMetadata();

  /**
   * An interface for process-specific metadata in a cluster using {@link LeaderFinder}. In general, this metadata will
   * contain information useful for the non-leaders (for example, the {@link java.net.URI} of the leader).
   */
  interface Metadata extends Serializable {
    /**
     * @return A short, sanitized name for the current process. In general, the name should only include characters
     *         that are valid in a hostname.
     */
    String getShortName();
  }

}
