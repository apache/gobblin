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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.SerializationUtils;


public class SingleHelixTask {

  private static final Logger _logger = LoggerFactory.getLogger(SingleHelixTask.class);

  private GobblinMultiTaskAttempt taskAttempt;
  private String jobId;
  private Path workUnitFilePath;
  private Path jobStateFilePath;
  private FileSystem fs;
  private TaskAttemptBuilder taskAttemptBuilder;
  private StateStores stateStores;

  public SingleHelixTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores) {
    this.jobId = jobId;
    this.workUnitFilePath = workUnitFilePath;
    this.jobStateFilePath = jobStateFilePath;
    this.fs = fs;
    this.taskAttemptBuilder = taskAttemptBuilder;
    this.stateStores = stateStores;
  }

  public int run()
      throws IOException, InterruptedException {
    SharedResourcesBroker<GobblinScopeTypes> globalBroker = null;
    try {
      return runInternal(globalBroker);
    }finally {
      closeGlobalBroker(globalBroker);
    }
  }

  private int runInternal(SharedResourcesBroker<GobblinScopeTypes> globalBroker)
      throws IOException, InterruptedException {
    List<WorkUnit> workUnits = getWorkUnits();
    int workUnitSize = workUnits.size();

    JobState jobState = getJobState();

    globalBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.parseProperties(jobState.getProperties()), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<GobblinScopeTypes> jobBroker =
        globalBroker.newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId())).build();

    this.taskAttempt = this.taskAttemptBuilder.build(workUnits.iterator(), this.jobId, jobState, jobBroker);
    this.taskAttempt.runAndOptionallyCommitTaskAttempt(GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE);
    return workUnitSize;

  }
  private void closeGlobalBroker(SharedResourcesBroker<GobblinScopeTypes> globalBroker) {
    if (globalBroker != null) {
      try {
        globalBroker.close();
      } catch (IOException ioe) {
        _logger.error("Could not close shared resources broker.", ioe);
      }
    }
  }

  private JobState getJobState()
      throws java.io.IOException {
    JobState jobState = new JobState();
    SerializationUtils.deserializeState(this.fs, jobStateFilePath, jobState);
    return jobState;
  }

  private List<WorkUnit> getWorkUnits()
      throws IOException {
    String fileName = workUnitFilePath.getName();
    String storeName = workUnitFilePath.getParent().getName();
    WorkUnit workUnit;

    if (workUnitFilePath.getName().endsWith(AbstractJobLauncher.MULTI_WORK_UNIT_FILE_EXTENSION)) {
      workUnit = stateStores.mwuStateStore.getAll(storeName, fileName).get(0);
    } else {
      workUnit = stateStores.wuStateStore.getAll(storeName, fileName).get(0);
    }

    // The list of individual WorkUnits (flattened) to run
    List<WorkUnit> workUnits = Lists.newArrayList();

    if (workUnit instanceof MultiWorkUnit) {
      // Flatten the MultiWorkUnit so the job configuration properties can be added to each individual WorkUnits
      List<WorkUnit> flattenedWorkUnits = JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits());
      workUnits.addAll(flattenedWorkUnits);
    } else {
      workUnits.add(workUnit);
    }
    return workUnits;
  }

  public void cancel() {
    if (this.taskAttempt != null) {
      try {
        _logger.info("Task cancelled: Shutdown starting for tasks with jobId: {}", this.jobId);
        this.taskAttempt.shutdownTasks();
        _logger.info("Task cancelled: Shutdown complete for tasks with jobId: {}", this.jobId);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while shutting down task with jobId: " + this.jobId, e);
      }
    } else {
      _logger.error("Task cancelled but taskAttempt is null, so ignoring.");
    }
  }
}
