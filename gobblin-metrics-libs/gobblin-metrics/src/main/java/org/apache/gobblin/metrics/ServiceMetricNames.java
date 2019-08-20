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
  private static final String GOBBLIN_SERVICE_PREFIX = "gobblin.service.";

  // Flow Compilation Meters and Timer
  public static final String FLOW_COMPILATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.successful";
  public static final String FLOW_COMPILATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.failed";
  public static final String FLOW_COMPILATION_TIMER = GOBBLIN_SERVICE_PREFIX + "flowCompilation.time";

  // Flow Orchestration Meters and Timer
  public static final String FLOW_ORCHESTRATION_SUCCESSFUL_METER = GOBBLIN_SERVICE_PREFIX + "flowOrchestration.successful";
  public static final String FLOW_ORCHESTRATION_FAILED_METER = GOBBLIN_SERVICE_PREFIX + "flowOrchestration.failed";
  public static final String FLOW_ORCHESTRATION_TIMER = GOBBLIN_SERVICE_PREFIX + "flowOrchestration.time";

  //Job status poll timer
  public static final String JOB_STATUS_POLLED_TIMER = GOBBLIN_SERVICE_PREFIX + "jobStatusPoll.time";

  public static final String CREATE_FLOW_METER = "CreateFlow";
  public static final String DELETE_FLOW_METER = "DeleteFlow";
  public static final String RUN_IMMEDIATELY_FLOW_METER = "RunImmediatelyFlow";

  public static final String RUNNING_FLOWS_COUNTER = "RunningFlows";
  public static final String COMPILED = "Compiled";
}
