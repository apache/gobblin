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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.SerializationUtils;


/**
 * A standalone unit to initialize and execute {@link GobblinMultiTaskAttempt} through deserialized {@link WorkUnit}
 */
public class SingleTask {

  private static final Logger _logger = LoggerFactory.getLogger(SingleTask.class);

  public static final String MAX_RETRY_WAITING_FOR_INIT_KEY = "maxRetryBlockedOnTaskAttemptInit";
  public static final int DEFAULT_MAX_RETRY_WAITING_FOR_INIT = 2;

  @VisibleForTesting
  GobblinMultiTaskAttempt _taskAttempt;
  private String _jobId;
  private Path _workUnitFilePath;
  private Path _jobStateFilePath;
  private FileSystem _fs;
  private TaskAttemptBuilder _taskAttemptBuilder;
  private StateStores _stateStores;
  private Config _dynamicConfig;
  private JobState _jobState;

  // Preventing Helix calling cancel before taskAttempt is created
  // Checking if taskAttempt is empty is not enough, since canceller runs in different thread as runner, the case to
  // to avoid here is taskAttempt being created and start to run after cancel has been called.
  private Condition _taskAttemptBuilt;
  private Lock _lock;

  SingleTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores, Config dynamicConfig) {
    this(jobId, workUnitFilePath, jobStateFilePath, fs, taskAttemptBuilder, stateStores, dynamicConfig, false);
  }

  /**
   * Do all heavy-lifting of initialization in constructor which could be retried if failed,
   * see the example in {@link GobblinHelixTask}.
   */
  SingleTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores, Config dynamicConfig, boolean skipGetJobState) {
    _jobId = jobId;
    _workUnitFilePath = workUnitFilePath;
    _jobStateFilePath = jobStateFilePath;
    _fs = fs;
    _taskAttemptBuilder = taskAttemptBuilder;
    _stateStores = stateStores;
    _dynamicConfig = dynamicConfig;
    _lock = new ReentrantLock();
    _taskAttemptBuilt = _lock.newCondition();

    if (!skipGetJobState) {
      try {
        _jobState = getJobState();
      } catch (IOException ioe) {
        throw new RuntimeException("Failing in deserializing jobState...", ioe);
      }
    } else {
      this._jobState = null;
    }
  }

  public void run()
      throws IOException, InterruptedException {

    if (_jobState == null) {
      throw new RuntimeException("jobState is null. Task may have already been cancelled.");
    }

    // Add dynamic configuration to the job state
    _dynamicConfig.entrySet().forEach(e -> _jobState.setProp(e.getKey(), e.getValue().unwrapped().toString()));

    Config jobConfig = getConfigFromJobState(_jobState);

    _logger.debug("SingleTask.run: jobId {} workUnitFilePath {} jobStateFilePath {} jobState {} jobConfig {}",
        _jobId, _workUnitFilePath, _jobStateFilePath, _jobState, jobConfig);

    try (SharedResourcesBroker<GobblinScopeTypes> globalBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(jobConfig, GobblinScopeTypes.GLOBAL.defaultScopeInstance())) {
      SharedResourcesBroker<GobblinScopeTypes> jobBroker = getJobBroker(_jobState, globalBroker);

      // Secure atomicity of taskAttempt's execution.
      // Signaling blocking threads if any whenever taskAttempt is nonNull.
      _taskAttempt = _taskAttemptBuilder.build(getWorkUnits().iterator(), _jobId, _jobState, jobBroker);

      _lock.lock();
      try {
        _taskAttemptBuilt.signal();
      } finally {
        _lock.unlock();
      }

      // This is a blocking call.
      _taskAttempt.runAndOptionallyCommitTaskAttempt(GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE);

    } finally {
      _logger.info("Clearing all metrics object in cache.");
      _taskAttempt.cleanMetrics();
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

  protected JobState getJobState() throws IOException {
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

  /**
   * Deserialize {@link WorkUnit}s from a path.
   */
  protected List<WorkUnit> getWorkUnits()
      throws IOException {
    String fileName = _workUnitFilePath.getName();
    String storeName = _workUnitFilePath.getParent().getName();
    WorkUnit workUnit;

    try {
      if (_workUnitFilePath.getName().endsWith(JobLauncherUtils.MULTI_WORK_UNIT_FILE_EXTENSION)) {
        workUnit = _stateStores.getMwuStateStore().getAll(storeName, fileName).get(0);
      } else {
        workUnit = _stateStores.getWuStateStore().getAll(storeName, fileName).get(0);
      }
    } catch (IOException e) {
      //Add workunitFilePath to the IOException message to aid debugging
      throw new IOException("Exception retrieving state from state store for workunit: " + _workUnitFilePath.toString(),
          e);
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
    int retryCount = 0 ;
    int maxRetry = ConfigUtils.getInt(_dynamicConfig, MAX_RETRY_WAITING_FOR_INIT_KEY, DEFAULT_MAX_RETRY_WAITING_FOR_INIT);

    try {
      _lock.lock();
      try {
        while (_taskAttempt == null) {
          // await return false if timeout on this around
          if (!_taskAttemptBuilt.await(5, TimeUnit.SECONDS) && ++retryCount > maxRetry) {
            throw new IllegalStateException("Failed to initialize taskAttempt object before cancel");
          }
        }
      } finally {
        _lock.unlock();
      }

      if (_taskAttempt != null) {
        _logger.info("Task cancelled: Shutdown starting for tasks with jobId: {}", _jobId);
        _taskAttempt.shutdownTasks();
        _logger.info("Task cancelled: Shutdown complete for tasks with jobId: {}", _jobId);
      } else {
        throw new IllegalStateException("TaskAttempt not initialized while passing conditional barrier");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while shutting down task with jobId: " + _jobId, e);
    }
  }
}
