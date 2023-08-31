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

package org.apache.gobblin.service.monitoring.event;

import lombok.Getter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * An object that {@link org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor} emits when it receives a final
 * job status GTE.
 */
@Getter
public class JobStatusEvent {
  State jobStatusState;
  String flowGroup;
  String flowName;
  long flowExecutionId;
  String jobGroup;
  String jobName;
  ExecutionStatus status;
  JobStatus jobStatus;
  public JobStatusEvent(State jobStatusState) {
    this.jobStatusState = jobStatusState;
    this.status = ExecutionStatus.valueOf(jobStatusState.getProp(JobStatusRetriever.EVENT_NAME_FIELD));
    this.flowName = jobStatusState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    this.flowGroup = jobStatusState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    this.flowExecutionId = jobStatusState.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD);
    this.jobName = jobStatusState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
    this.jobGroup = jobStatusState.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
    this.jobStatus = JobStatusRetriever.createJobStatusBuilderFromState(jobStatusState).build();
  }
}
