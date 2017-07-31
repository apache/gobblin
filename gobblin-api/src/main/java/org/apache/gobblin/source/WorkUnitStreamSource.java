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

package org.apache.gobblin.source;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.workunit.WorkUnitStream;


/**
 * A {@link Source} that produces a {@link WorkUnitStream}.
 *
 * Streamed sources may offer certain advantages over non-streamed sources if the job launcher supports them:
 *  * Low memory usage: If the {@link WorkUnitStream} is backed by a generator, the job launcher may optimize memory
 *    usage by never materializing all work units in memory.
 *  * Eager processing of slow sources: If the source is slow at producing work units, the job launcher may start
 *    processing available work units while future work units are still being computed.
 *  * Infinite work unit streams: in the future, some job launchers will support infinite streams of work units.
 */
public interface WorkUnitStreamSource<S, D> extends Source<S, D> {
  /**
   * Get the {@link WorkUnitStream} to process.
   */
  WorkUnitStream getWorkunitStream(SourceState state);
}
