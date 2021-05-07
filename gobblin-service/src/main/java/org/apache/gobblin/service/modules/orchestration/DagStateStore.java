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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface for storing and retrieving currently running {@link Dag<JobExecutionPlan>}s. In case of a leadership
 * change in the {@link org.apache.gobblin.service.modules.core.GobblinServiceManager}, the corresponding {@link DagManager}
 * loads the running {@link Dag}s from the {@link DagStateStore} to resume their execution.
 */
@Alpha
public interface DagStateStore {
  /**
   * Persist the {@link Dag} to the backing store.
   * This is not an actual checkpoint but more like a Write-ahead log, where uncommitted job will be persisted
   * and be picked up again when leader transition happens.
   * @param dag The dag submitted to {@link DagManager}
   */
  void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dag The dag completed/cancelled from execution on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  void cleanUp(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dagId The ID of the dag to clean up.
   */
  void cleanUp(String dagId) throws IOException;

  /**
   * Load all currently running {@link Dag}s from the underlying store. Typically, invoked when a new {@link DagManager}
   * takes over or on restart of service.
   * @return a {@link List} of currently running {@link Dag}s.
   */
  List<Dag<JobExecutionPlan>> getDags() throws IOException;

  /**
   * Return a single dag from the dag state store.
   * @param dagId The ID of the dag to load.
   */
  Dag<JobExecutionPlan> getDag(String dagId) throws IOException;

  /**
   * Return a list of all dag IDs contained in the dag state store.
   */
  Set<String> getDagIds() throws IOException;
}
