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
import java.util.concurrent.ExecutionException;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;


/**
 * Responsible for defining the behavior of {@link DagTask} handling scenarios for launch, resume, kill, job start
 * and flow completion deadlines
 *
 */
@Alpha
public interface DagManagement {

  /**
   * Currently, it is handling just the launch of a {@link Dag} request via REST client for adhoc flows.
   * The eventTimestamp for adhoc flows will always be 0 (zero), while -1 would indicate failures.
   * Essentially for a valid launch flow, the eventTimestamp needs to be >= 0.
   * Future implementations will cover launch of flows through the scheduler too!
   * @param flowGroup
   * @param flowName
   * @param eventTimestamp
   */
  void launchFlow(String flowGroup, String flowName, long  eventTimestamp);

  /**
   * Handles the resume of a {@link Dag} request via REST client or triggered by the scheduler.
   * @param resumeAction
   * @param eventTimestamp
   * @throws IOException
   */
  void resumeFlow(DagActionStore.DagAction resumeAction, long  eventTimestamp) throws IOException;

  /**
   * Currently, it is handling just the kill/cancel of a {@link Dag} request via REST client
   * @param killAction
   * @param eventTimestamp
   * @throws IOException
   */
  void killFlow(DagActionStore.DagAction killAction, long eventTimestamp) throws IOException;

  boolean enforceFlowCompletionDeadline(Dag.DagNode<JobExecutionPlan> node) throws ExecutionException, InterruptedException;

  boolean enforceJobStartDeadline(Dag.DagNode<JobExecutionPlan> node, JobStatus jobStatus) throws ExecutionException, InterruptedException;
}
