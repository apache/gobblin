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
package org.apache.gobblin.metrics;

public class ServiceMetricNames {
  // These prefixes can be used to distinguish metrics reported by GobblinService from other metrics reported by Gobblin
  // This can be used in conjunction with MetricNameRegexFilter to filter out metrics in any MetricReporter
  public static final String GOBBLIN_SERVICE_PREFIX = "GobblinService";
  public static final String GOBBLIN_JOB_METRICS_PREFIX = "JobMetrics";

  // Flow Compilation Meters and Timer
  public static final String FLOW_COMPILATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.successful";
  public static final String FLOW_COMPILATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.failed";
  public static final String FLOW_COMPILATION_TIMER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.time";
  public static final String DATA_AUTHORIZATION_TIMER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.dataAuthorization.time";

  // Flow Orchestration Meters and Timer
  public static final String FLOW_ORCHESTRATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX + ".flowOrchestration.successful";
  public static final String FLOW_ORCHESTRATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX + ".flowOrchestration.failed";
  public static final String FLOW_ORCHESTRATION_TIMER = GOBBLIN_SERVICE_PREFIX + ".flowOrchestration.time";
  public static final String FLOW_ORCHESTRATION_DELAY = GOBBLIN_SERVICE_PREFIX + ".flowOrchestration.delay";

  //Job status poll timer
  public static final String JOB_STATUS_POLLED_TIMER = GOBBLIN_SERVICE_PREFIX + ".jobStatusPoll.time";

  public static final String CREATE_FLOW_METER = "CreateFlow";
  public static final String DELETE_FLOW_METER = "DeleteFlow";
  public static final String RUN_IMMEDIATELY_FLOW_METER = "RunImmediatelyFlow";
  public static final String SUCCESSFUL_FLOW_METER = "SuccessfulFlows";
  public static final String START_SLA_EXCEEDED_FLOWS_METER = "StartSLAExceededFlows";
  public static final String SLA_EXCEEDED_FLOWS_METER = "SlaExceededFlows";
  public static final String FAILED_FLOW_METER = "FailedFlows";
  public static final String SCHEDULED_FLOW_METER = GOBBLIN_SERVICE_PREFIX + ".ScheduledFlows";
  public static final String NON_SCHEDULED_FLOW_METER = GOBBLIN_SERVICE_PREFIX + ".NonScheduledFlows";
  public static final String SKIPPED_FLOWS = GOBBLIN_SERVICE_PREFIX + ".SkippedFlows";
  public static final String RUNNING_FLOWS_COUNTER = "RunningFlows";
  public static final String SERVICE_USERS = "ServiceUsers";
  public static final String COMPILED = "Compiled";
  public static final String RUNNING_STATUS = "RunningStatus";
  public static final String JOBS_SENT_TO_SPEC_EXECUTOR = "JobsSentToSpecExecutor";

  public static final String HELIX_LEADER_STATE = "HelixLeaderState";

  public static final String FLOWGRAPH_UPDATE_FAILED_METER = GOBBLIN_SERVICE_PREFIX + ".FlowgraphUpdateFailed";
}
