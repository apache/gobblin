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

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.temporal.dynamic.FsScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;


/**
 * {@link FsScalingDirectiveSource} based implementation of {@link AbstractDynamicScalingYarnServiceManager}.
 */
public class FsSourceDynamicScalingYarnServiceManager extends AbstractDynamicScalingYarnServiceManager {

  public FsSourceDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    super(appMaster);
  }

  @Override
  protected ScalingDirectiveSource createScalingDirectiveSource() throws IOException {
    JobState jobState = new JobState(ConfigUtils.configToProperties(this.config));
    FileSystem fs = JobStateUtils.openFileSystem(jobState);
    return new FsScalingDirectiveSource(
        fs,
        JobStateUtils.getDynamicScalingPath(JobStateUtils.getWorkDirRoot(jobState)),
        Optional.of(JobStateUtils.getDynamicScalingErrorsPath(JobStateUtils.getWorkDirRoot(jobState)))
    );
  }
}
