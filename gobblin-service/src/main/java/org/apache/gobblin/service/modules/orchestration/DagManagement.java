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
   * Currently, it is handling just the launch of a {@link Dag} request via REST client for adhoc flows
   * @param flowGroup
   * @param flowName
   * @param triggerTimeStamp
   */
  void launchFlow(String flowGroup, String flowName, long  triggerTimeStamp);

  /**
   * Currently, it is handling just the resume of a {@link Dag} request via REST client for adhoc flows
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param triggerTimeStamp
   * @throws IOException
   */
  void resumeFlow(String flowGroup, String flowName, String flowExecutionId, long  triggerTimeStamp)
      throws IOException, InterruptedException;

  /**
   * Currently, it is handling just the kill/cancel of a {@link Dag} request via REST client for adhoc flows
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param triggerTimeStamp
   */
  void killFlow(String flowGroup, String flowName, String flowExecutionId, long  triggerTimeStamp)
      throws InterruptedException, IOException;

  boolean enforceFlowCompletionDeadline(Dag.DagNode<JobExecutionPlan> node) throws ExecutionException, InterruptedException;

  boolean enforceJobStartDeadline(Dag.DagNode<JobExecutionPlan> node, JobStatus jobStatus) throws ExecutionException, InterruptedException;
}
