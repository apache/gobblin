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
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
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


public class SingleTask {

  private static final Logger _logger = LoggerFactory.getLogger(SingleTask.class);

  private GobblinMultiTaskAttempt _taskattempt;
  private String _jobId;
  private Path _workUnitFilePath;
  private Path _jobStateFilePath;
  private FileSystem _fs;
  private TaskAttemptBuilder _taskAttemptBuilder;
  private StateStores _stateStores;

  SingleTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores) {
    _jobId = jobId;
    _workUnitFilePath = workUnitFilePath;
    _jobStateFilePath = jobStateFilePath;
    _fs = fs;
    _taskAttemptBuilder = taskAttemptBuilder;
    _stateStores = stateStores;
  }

  public void run()
      throws IOException, InterruptedException {
    List<WorkUnit> workUnits = getWorkUnits();

    JobState jobState = getJobState();
    Config jobConfig = getConfigFromJobState(jobState);

    _logger.debug("SingleTask.run: jobId {} workUnitFilePath {} jobStateFilePath {} jobState {} jobConfig {}",
        _jobId, _workUnitFilePath, _jobStateFilePath, jobState, jobConfig);

    try (SharedResourcesBroker<GobblinScopeTypes> globalBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(jobConfig, GobblinScopeTypes.GLOBAL.defaultScopeInstance())) {
      SharedResourcesBroker<GobblinScopeTypes> jobBroker = getJobBroker(jobState, globalBroker);

      _taskattempt = _taskAttemptBuilder.build(workUnits.iterator(), _jobId, jobState, jobBroker);
      _taskattempt.runAndOptionallyCommitTaskAttempt(GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE);
    }
  }

  private SharedResourcesBroker<GobblinScopeTypes> getJobBroker(JobState jobState,
      SharedResourcesBroker<GobblinScopeTypes> globalBroker) {
    return globalBroker.newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId())).build();
  }

  private Config getConfigFromJobState(JobState jobState) {
    Properties jobProperties = jobState.getProperties();
    return ConfigFactory.parseProperties(jobProperties);
  }

  private JobState getJobState()
      throws java.io.IOException {
    JobState jobState;

    // read the state from the state store if present, otherwise deserialize directly from the file
    if (_stateStores.haveJobStateStore()) {
      jobState = _stateStores.getJobStateStore().get(_jobStateFilePath.getParent().getName(),
          _jobStateFilePath.getName(),
          _jobStateFilePath.getParent().getName());
    } else {
      jobState = new JobState();
      SerializationUtils.deserializeState(_fs, _jobStateFilePath, jobState);
    }

    return jobState;
  }

  private List<WorkUnit> getWorkUnits()
      throws IOException {
    String fileName = _workUnitFilePath.getName();
    String storeName = _workUnitFilePath.getParent().getName();
    WorkUnit workUnit;

    if (_workUnitFilePath.getName().endsWith(AbstractJobLauncher.MULTI_WORK_UNIT_FILE_EXTENSION)) {
      workUnit = _stateStores.getMwuStateStore().getAll(storeName, fileName).get(0);
    } else {
      workUnit = _stateStores.getWuStateStore().getAll(storeName, fileName).get(0);
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
    if (_taskattempt != null) {
      try {
        _logger.info("Task cancelled: Shutdown starting for tasks with jobId: {}", _jobId);
        _taskattempt.shutdownTasks();
        _logger.info("Task cancelled: Shutdown complete for tasks with jobId: {}", _jobId);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while shutting down task with jobId: " + _jobId, e);
      }
    } else {
      _logger.error("Task cancelled but _taskattempt is null, so ignoring.");
    }
  }
}
