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

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;


/**
 * Conveys a {@link org.apache.gobblin.source.workunit.WorkUnit} by claim-check, where the `workUnitPath` is resolved
 * against the {@link org.apache.hadoop.fs.FileSystem} given by `nameNodeUri`.  see:
 * @see <a href="https://learn.microsoft.com/en-us/azure/architecture/patterns/claim-check">Claim-Check Pattern</a>
 */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WorkUnitClaimCheck implements FileSystemApt, FileSystemJobStateful {
  @NonNull private String correlator;
  @NonNull private URI fileSystemUri;
  @NonNull private String workUnitPath;
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
