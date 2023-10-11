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
import lombok.extern.slf4j.Slf4j;

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


@Slf4j
public class JobStateUtils {
  private static final String OUTPUT_DIR_NAME = "output"; // following MRJobLauncher.OUTPUT_DIR_NAME

  private JobStateUtils() {}

  public static StateStore<TaskState> openTaskStateStore(JobState jobState, FileSystem fs) {
    Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fs);
    log.info("opening FS task state store at path '{}'", taskStateStorePath);
    return new FsStateStore<>(fs, taskStateStorePath.toUri().getPath(), TaskState.class);
  }

  //!!!!!document dependence on MR_JOB_ROOT_DIR_KEY = "mr.job.root.dir";
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
