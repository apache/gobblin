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

package org.apache.gobblin.temporal.ddm.util;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;


/**
 * Utilities for applying {@link JobState} info to various ends:
 * - creating a {@link SharedResourcesBroker}
 * - obtaining a {@link StateStore<TaskState>}
 */
@Slf4j
public class JobStateUtils {
  private static final String OUTPUT_DIR_NAME = "output"; // following MRJobLauncher.OUTPUT_DIR_NAME

  // reuse same handle among activities executed by the same worker
  private static final transient Cache<Path, StateStore<TaskState>> taskStateStoreByPath = CacheBuilder.newBuilder().build();

  private JobStateUtils() {}

  public static StateStore<TaskState> openTaskStateStore(JobState jobState, FileSystem fs) {
    try {
      Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fs);
      return taskStateStoreByPath.get(taskStateStorePath, () ->
          openTaskStateStoreUncached(jobState, fs)
      );
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  public static StateStore<TaskState> openTaskStateStoreUncached(JobState jobState, FileSystem fs) {
    Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fs);
    log.info("opening FS task state store at path '{}'", taskStateStorePath);
    return new FsStateStore<>(fs, taskStateStorePath.toUri().getPath(), TaskState.class);
  }

  /**
   * ATTENTION: derives path according to {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher} conventions, using same
   * {@link ConfigurationKeys#MR_JOB_ROOT_DIR_KEY}
   * @return path to {@link FsStateStore<TaskState>} backing dir
   */
  public static Path getTaskStateStorePath(JobState jobState, FileSystem fs) {
    Properties jobProps = jobState.getProperties();
    Path jobOutputPath = new Path(
        new Path(
            new Path(
                jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY),
                JobState.getJobNameFromProps(jobProps)),
            JobState.getJobIdFromProps(jobProps)),
        OUTPUT_DIR_NAME);
    return fs.makeQualified(jobOutputPath);
  }

  public static SharedResourcesBroker<GobblinScopeTypes> getSharedResourcesBroker(JobState jobState) {
    SharedResourcesBroker<GobblinScopeTypes> globalBroker =
        SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
            ConfigFactory.parseProperties(jobState.getProperties()),
            GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    return globalBroker.newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId())).build();
  }
}
