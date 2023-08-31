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
import java.util.Map;
import java.util.Set;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.KillFlowEvent;
import org.apache.gobblin.service.monitoring.ResumeFlowEvent;
import org.apache.gobblin.service.monitoring.event.JobStatusEvent;


public interface DagManagement {
  Map<String, Set<Dag.DagNode<JobExecutionPlan>>> onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;
  void removeDagActionFromStore(DagActionStore.DagAction dagAction) throws IOException;
  void handleJobStatusEvent(JobStatusEvent jobStatusEvent);
  void handleKillFlowEvent(KillFlowEvent killFlowEvent);
  void handleResumeFlowEvent(ResumeFlowEvent resumeFlowEvent) throws IOException;
  DagStateStore getDagStateStore();
  void setActive() throws IOException;
}
