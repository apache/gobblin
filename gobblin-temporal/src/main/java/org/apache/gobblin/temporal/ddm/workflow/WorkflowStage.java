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
 * Each stage has independent resource configuration (memory, OOM limits) and dedicated containers.
 *
 * <p>Stages:
 * <ul>
 *   <li>WORK_DISCOVERY: Discovers data sources, generates work units, and handles commits (1 container, lightweight operations)</li>
 *   <li>WORK_EXECUTION: Processes work units to transform and load data (20+ containers)</li>
 * </ul>
 *
 * <p>Each stage has:
 * <ul>
 *   <li>Dedicated task queue for activity routing</li>
 *   <li>Specialized worker class</li>
 *   <li>Independent memory configuration</li>
 *   <li>Stage-specific OOM handling</li>
 * </ul>
 */
@Getter
public enum WorkflowStage {
  WORK_DISCOVERY("workDiscovery", GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_DISCOVERY_COMMIT_TASK_QUEUE),
  WORK_EXECUTION("workExecution", GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE),
  COMMIT("commit", GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_TASK_QUEUE,
      GobblinTemporalConfigurationKeys.DEFAULT_DISCOVERY_COMMIT_TASK_QUEUE);

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
   * @param profileName the profile name (e.g., "workExecution-proc", "baseline-workDiscovery")
   * @return the corresponding WorkflowStage
   */
  public static WorkflowStage fromProfileName(String profileName) {
    if (profileName == null || profileName.isEmpty()) {
      return WORK_DISCOVERY;  // Global baseline used by initial container
    }
    for (WorkflowStage stage : values()) {
      if (profileName.contains(stage.getProfileBaseName())) {
        return stage;
      }
    }
    return WORK_EXECUTION;  // Default to execution if no match
  }
}
