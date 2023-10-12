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
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.fs.Path;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.util.nesting.work.WFAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;


/**
 * Intended to reference multiple {@link org.apache.gobblin.source.workunit.WorkUnit}s to process, where `workUnitsDir`
 * is resolved against the {@link org.apache.hadoop.fs.FileSystem} given by `nameNodeUri`.  see:
 */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WUProcessingSpec implements FileSystemApt, FileSystemJobStateful {
  @NonNull private URI fileSystemUri;
  @NonNull private String workUnitsDir;
  @NonNull private Tuning tuning = Tuning.DEFAULT;

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  @Override
  public State getFileSystemConfig() {
    return new State(); // TODO!
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  @Override
  public Path getJobStatePath() {
    // TODO: decide whether wise to hard-code... (per `MRJobLauncher` conventions, we expect job state file to be sibling of WU dir)
    return new Path(new Path(workUnitsDir).getParent(), AbstractJobLauncher.JOB_STATE_FILE_NAME);
  }

  /** Configuration for {@link org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow#performWorkload(WFAddr, Workload, int, int, int, Optional)}*/
  @Data
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  @RequiredArgsConstructor
  public static class Tuning {
    public static int DEFAULT_MAX_BRANCHES_PER_TREE = 900;
    public static int DEFAULT_SUB_TREES_PER_TREE = 30;

    public static Tuning DEFAULT = new Tuning(DEFAULT_MAX_BRANCHES_PER_TREE, DEFAULT_SUB_TREES_PER_TREE);

    @NonNull private int maxBranchesPerTree;
    @NonNull private int maxSubTreesPerTree;
  }
}
