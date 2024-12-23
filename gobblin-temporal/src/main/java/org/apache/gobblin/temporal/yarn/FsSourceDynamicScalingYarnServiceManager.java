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

package org.apache.gobblin.temporal.yarn;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.dynamic.FsScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * {@link FsScalingDirectiveSource} based implementation of {@link AbstractDynamicScalingYarnServiceManager}.
 */
@Slf4j
public class FsSourceDynamicScalingYarnServiceManager extends AbstractDynamicScalingYarnServiceManager {

  private FileSystem fs;

  public FsSourceDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    super(appMaster);
  }

  @Override
  protected void startUp() throws IOException {
    JobState jobState = new JobState(ConfigUtils.configToProperties(this.config));
    // since `super.startUp()` will invoke `createScalingDirectiveSource()`, which needs `this.fs`, create it beforehand
    this.fs = JobStateUtils.openFileSystem(jobState);
    super.startUp();
  }

  @Override
  protected void shutDown() throws IOException {
    super.shutDown();
    this.fs.close();
  }

  @Override
  protected ScalingDirectiveSource createScalingDirectiveSource() throws IOException {
    String appName = config.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY);
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, fs, appName, this.applicationId);
    log.info("Using GobblinCluster work dir: {}", appWorkDir);
    return new FsScalingDirectiveSource(
        fs,
        JobStateUtils.getDynamicScalingPath(appWorkDir),
        Optional.of(JobStateUtils.getDynamicScalingErrorsPath(appWorkDir))
    );
  }
}
