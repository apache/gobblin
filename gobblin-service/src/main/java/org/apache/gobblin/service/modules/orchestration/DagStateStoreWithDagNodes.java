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
import java.util.Optional;
import java.util.Set;

import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface for storing and retrieving currently running {@link Dag.DagNode<JobExecutionPlan>}s.
 * Callers should use {@link DagStateStore#writeCheckpoint} to store dags. After that, to update individual
 * {@link Dag.DagNode}s, {@link DagStateStoreWithDagNodes#updateDagNode} should be used.
 * {@link DagStateStore#cleanUp(Dag.DagId)} should be used to delete all the {@link Dag.DagNode}s for a {@link Dag}.
 */
public interface DagStateStoreWithDagNodes extends DagStateStore {

  /**
   * Updates the {@link Dag.DagNode} with the provided value.
   * Returns true if the dag node is updated successfully, false otherwise
   */
  boolean updateDagNode(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;

  /**
   * Returns all the {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode}s for the given
   * {@link Dag.DagId}
   */
  Set<Dag.DagNode<JobExecutionPlan>> getDagNodes(Dag.DagId dagId) throws IOException;

  /**
   * Return the {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} for the given {@link DagNodeId} or empty
   * optional if it is not present
   */
  Optional<Dag.DagNode<JobExecutionPlan>> getDagNode(DagNodeId dagNodeId) throws IOException;
}
