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

package org.apache.gobblin.temporal.ddm.work;

import java.net.URI;

import org.apache.hadoop.fs.Path;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.util.WorkUnitSizeInfo;


/**
 * Conveys a {@link org.apache.gobblin.source.workunit.WorkUnit} by claim-check, where the `workUnitPath` is resolved
 * against the {@link org.apache.hadoop.fs.FileSystem} given by `nameNodeUri`.  see:
 * @see <a href="https://learn.microsoft.com/en-us/azure/architecture/patterns/claim-check">Claim-Check Pattern</a>
 *
 * TODO: if we're to generalize Work Prediction+Prioritization across multiplexed jobs, each having its own separate time budget, every WU claim-check
 * standing on its own would allow an external observer to inspect only the task queue w/o correlation between workflow histories.  For that, decide whether
 * to add job-identifying metadata here or just tack on time budget (aka. SLA deadline) info.  Either could be tunneled within the filename in the manner
 * of `JobLauncherUtils.WorkUnitPathCalculator.calcNextPathWithTunneledSizeInfo` - in fact, by convention, the job ID / flow ID already is... we just don't
 * recover it herein.
 */
@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WorkUnitClaimCheck implements FileSystemApt, FileSystemJobStateful {
  @NonNull @Setter(AccessLevel.PACKAGE) private String correlator;
  @NonNull private URI fileSystemUri;
  @NonNull private String workUnitPath;
  @NonNull private WorkUnitSizeInfo workUnitSizeInfo;
  @NonNull private EventSubmitterContext eventSubmitterContext;

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  @Override
  public State getFileSystemConfig() {
    return new State(); // TODO - figure out how to truly set!
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  @Override
  public Path getJobStatePath() {
    // TODO: decide whether wise to hard-code... (per `MRJobLauncher` conventions, we expect job state file to be sibling of WU dir)
    return new Path(new Path(workUnitPath).getParent().getParent(), AbstractJobLauncher.JOB_STATE_FILE_NAME);
  }
}
