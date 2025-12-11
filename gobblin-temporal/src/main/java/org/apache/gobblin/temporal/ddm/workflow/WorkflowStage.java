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

package org.apache.gobblin.temporal.ddm.workflow;

import lombok.Getter;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;

/**
 * Represents the different stages of a Gobblin Temporal workflow.
 * 
 * <p>Stages:
 * <ul>
 *   <li>WORK_DISCOVERY: Discovers data sources, generates work units (uses default queue)</li>
 *   <li>WORK_EXECUTION: Processes work units to transform and load data (uses execution queue when dynamic scaling enabled)</li>
 *   <li>COMMIT: Commits work units (uses default queue)</li>
 * </ul>
 *
 * <p>Queue routing:
 * <ul>
 *   <li>Dynamic scaling OFF: All stages use default queue</li>
 *   <li>Dynamic scaling ON: WORK_EXECUTION uses dedicated execution queue, others use default queue</li>
 * </ul>
 */
@Getter
public enum WorkflowStage {
  WORK_DISCOVERY("workDiscovery", GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE),
  WORK_EXECUTION("workExecution", GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE),
  COMMIT("commit", GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

  private final String profileBaseName;
  private final String taskQueueConfigKey;
  private final String defaultTaskQueue;

  WorkflowStage(String profileBaseName, String taskQueueConfigKey, String defaultTaskQueue) {
    this.profileBaseName = profileBaseName;
    this.taskQueueConfigKey = taskQueueConfigKey;
    this.defaultTaskQueue = defaultTaskQueue;
  }

  /**
   * Returns the baseline profile name pattern for this stage.
   * Used for naming derived profiles and identifying stage from profile names.
   * Note: Stage-specific profiles are derived dynamically from the global baseline,
   * not created as separate baseline profiles.
   * Example: "baseline-workDiscovery", "baseline-workExecution"
   */
  public String getBaselineProfileName() {
    return "baseline-" + profileBaseName;
  }

  /**
   * Returns the processing profile name for this stage.
   * Example: "workDiscovery-proc", "workExecution-proc"
   */
  public String getProcessingProfileName() {
    return profileBaseName + "-proc";
  }

  /**
   * Returns the task queue for this stage, reading from config or using default.
   * Example: "GobblinTemporalDiscoveryCommitQueue", "GobblinTemporalExecutionQueue"
   *
   * @param config the configuration to read from
   * @return the task queue name for this stage
   */
  public String getTaskQueue(com.typesafe.config.Config config) {
    return config.hasPath(taskQueueConfigKey)
        ? config.getString(taskQueueConfigKey)
        : defaultTaskQueue;
  }

  /**
   * Determines the workflow stage from a profile name.
   * Used by OOM handler to identify which stage a container belongs to.
   *
   * In the 2-worker model:
   * - Empty/null profile name = baseline/default containers (WorkFulfillmentWorker) → WORK_DISCOVERY
   * - Profile containing "workExecution" or "execution" = ExecutionWorker → WORK_EXECUTION
   * - All other profiles = default containers → WORK_DISCOVERY
   *
   * @param profileName the profile name (e.g., "initial-execution", "workExecution-oomReplacement-1", "" for baseline)
   * @return the corresponding WorkflowStage
   */
  public static WorkflowStage fromProfileName(String profileName) {
    // Empty or null = baseline/default containers (WorkFulfillmentWorker)
    if (profileName == null || profileName.isEmpty()) {
      return WORK_DISCOVERY;  // Represents default/non-execution work
    }
    
    // Check if this is an execution worker profile
    if (profileName.contains("workExecution") || profileName.contains("execution")) {
      return WORK_EXECUTION;
    }
    
    // All other profiles default to non-execution work (WorkFulfillmentWorker)
    return WORK_DISCOVERY;
  }
}
