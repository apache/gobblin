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
package gobblin.runtime.std;

import java.net.URI;

import gobblin.annotation.Alpha;
import gobblin.runtime.JobState;
import gobblin.runtime.api.JobExecution;
import gobblin.runtime.api.JobSpec;
import gobblin.util.JobLauncherUtils;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Identifies a specific execution of a {@link JobSpec}
 */
@Alpha
@Data
@AllArgsConstructor
public class JobExecutionUpdatable implements JobExecution {
  /** The URI of the job being executed */
  protected final URI jobSpecURI;
  /** The version of the JobSpec being launched */
  protected final String jobSpecVersion;
  /** The millisecond timestamp when the job was launched */
  protected final long launchTimeMillis;
  /** Unique (for the given JobExecutionLauncher) id for this execution */
  protected final String executionId;

  public static JobExecutionUpdatable createFromJobSpec(JobSpec jobSpec) {
    return new JobExecutionUpdatable(jobSpec.getUri(),
        jobSpec.getVersion(),
        System.currentTimeMillis(),
        JobLauncherUtils.newJobId(JobState.getJobNameFromProps(jobSpec.getConfigAsProperties())));
  }
}
