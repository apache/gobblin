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
  public static final String GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER = GOBBLIN_SERVICE_PREFIX + ".";
  public static final String GOBBLIN_JOB_METRICS_PREFIX = "JobMetrics";

  // Flow Compilation Meters and Timer
  public static final String FLOW_COMPILATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowCompilation.successful";
  public static final String FLOW_COMPILATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowCompilation.failed";
  public static final String FLOW_COMPILATION_TIMER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowCompilation.time";
  public static final String DATA_AUTHORIZATION_TIMER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowCompilation.dataAuthorization.time";

  // Flow Orchestration Meters and Timer
  public static final String FLOW_ORCHESTRATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowOrchestration.successful";
  public static final String FLOW_ORCHESTRATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowOrchestration.failed";
  public static final String FLOW_ORCHESTRATION_TIMER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowOrchestration.time";
  public static final String FLOW_ORCHESTRATION_DELAY = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowOrchestration.delay";

  // Flow Trigger Handler
  public static final String FLOW_TRIGGER_HANDLER_PREFIX = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "flowTriggerHandler.";
  public static final String GOBBLIN_FLOW_TRIGGER_HANDLER_NUM_FLOWS_SUBMITTED = FLOW_TRIGGER_HANDLER_PREFIX + "numFlowsSubmitted";
  public static final String FLOW_TRIGGER_HANDLER_LEASE_OBTAINED_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "leaseObtained";
  public static final String FLOW_TRIGGER_HANDLER_LEASED_TO_ANOTHER_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "leasedToAnother";
  public static final String FLOW_TRIGGER_HANDLER_NO_LONGER_LEASING_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "noLongerLeasing";
  public static final String FLOW_TRIGGER_HANDLER_JOB_DOES_NOT_EXIST_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "jobDoesNotExistInScheduler";
  public static final String FLOW_TRIGGER_HANDLER_FAILED_TO_SET_REMINDER_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "failedToSetReminderCount";
  public static final String FLOW_TRIGGER_HANDLER_LEASES_OBTAINED_DUE_TO_REMINDER_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "leasesObtainedDueToReminderCount";
  public static final String FLOW_TRIGGER_HANDLER_FAILED_TO_RECORD_LEASE_SUCCESS_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "failedToRecordLeaseSuccessCount";
  public static final String FLOW_TRIGGER_HANDLER_RECORDED_LEASE_SUCCESS_COUNT = FLOW_TRIGGER_HANDLER_PREFIX + "recordedLeaseSuccessCount";

  // DagManager Related Metrics
  public static final String DAG_MANAGER_PREFIX = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "dagManager";
  public static final String
      DAG_MANAGER_FAILED_LAUNCH_EVENTS_ON_STARTUP_COUNT = DAG_MANAGER_PREFIX + ".failedLaunchEventsOnStartupCount";
  public static final String DAG_MANAGER_SUCCESSFUL_LAUNCH_EVENTS_ON_STARTUP_COUNT = DAG_MANAGER_PREFIX + ".successfulLaunchEventsOnActivationCount";
  public static final String FLOW_FAILED_FORWARD_TO_DAG_MANAGER_COUNT = DAG_MANAGER_PREFIX + ".flowFailedForwardToDagManagerCount";

  //Job status poll timer
  public static final String JOB_STATUS_POLLED_TIMER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "jobStatusPoll.time";

  public static final String CREATE_FLOW_METER = "CreateFlow";
  public static final String DELETE_FLOW_METER = "DeleteFlow";
  public static final String RUN_IMMEDIATELY_FLOW_METER = "RunImmediatelyFlow";
  public static final String SUCCESSFUL_FLOW_METER = "SuccessfulFlows";
  public static final String START_SLA_EXCEEDED_FLOWS_METER = "StartSLAExceededFlows";
  public static final String SLA_EXCEEDED_FLOWS_METER = "SlaExceededFlows";
  public static final String FAILED_FLOW_METER = "FailedFlows";
  public static final String SCHEDULED_FLOW_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "ScheduledFlows";
  public static final String NON_SCHEDULED_FLOW_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "NonScheduledFlows";
  public static final String SKIPPED_FLOWS = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "SkippedFlows";
  public static final String RUNNING_FLOWS_COUNTER = "RunningFlows";
  public static final String SERVICE_USERS = "ServiceUsers";
  public static final String COMPILED = "Compiled";
  public static final String RUNNING_STATUS = "RunningStatus";
  public static final String JOBS_SENT_TO_SPEC_EXECUTOR = "JobsSentToSpecExecutor";

  public static final String HELIX_LEADER_STATE = "HelixLeaderState";

  public static final String FLOWGRAPH_UPDATE_FAILED_METER = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "FlowgraphUpdateFailed";

  public static final String DAG_COUNT_MYSQL_DAG_STATE_COUNT = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "MysqlDagStateStore" + ".totalDagCount";

  public static final String DAG_COUNT_FS_DAG_STATE_COUNT = GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "FsDagStateStore" + ".totalDagCount";
}
