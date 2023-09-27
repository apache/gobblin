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

package org.apache.gobblin.temporal.loadgen.launcher;

import io.temporal.client.WorkflowOptions;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.gobblin.temporal.loadgen.work.IllustrationItem;
import org.apache.gobblin.temporal.loadgen.work.SimpleGeneratedWorkload;
import org.apache.gobblin.temporal.util.nesting.work.WFAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow;
import org.apache.hadoop.fs.Path;

import static org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX;


/**
 * A {@link JobLauncher} for the initial triggering of a Temporal workflow that generates arbitrary load of many
 * activities nested beneath a single subsuming super-workflow.  see: {@link NestingExecWorkflow}
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler#buildJobLauncher(Properties)} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
@Slf4j
public class GenArbitraryLoadJobLauncher extends GobblinTemporalJobLauncher {
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_NUM_ACTIVITIES = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "num.activities";
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_MAX_BRANCHES_PER_TREE = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "max.branches.per.tree";
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_MAX_SUB_TREES_PER_TREE = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "max.sub.trees.per.tree";

  public GenArbitraryLoadJobLauncher(
      Properties jobProps,
      Path appWorkDir,
      List<? extends Tag<?>> metadataTags,
      ConcurrentHashMap<String, Boolean> runningMap
  ) throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap);
  }

  @Override
  public void submitJob(List<WorkUnit> workunits) {
    int numActivities = Integer.valueOf(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_NUM_ACTIVITIES, "<<not-set>>"));
    int maxBranchesPerTree = Integer.valueOf(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_MAX_BRANCHES_PER_TREE, "<<not-set>>"));
    int maxSubTreesPerTree = Integer.valueOf(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_MAX_SUB_TREES_PER_TREE, "<<not-set>>"));

    Workload<IllustrationItem> workload = SimpleGeneratedWorkload.createAs(numActivities);
    WorkflowOptions options = WorkflowOptions.newBuilder().setTaskQueue(this.queueName).build();

    // WARNING: although type param must agree w/ that of `workload`, it's entirely unverified by type checker!
    // ...and more to the point, mismatch would occur at runtime (`performWorkload` on the workflow type given to the stub)!
    NestingExecWorkflow<IllustrationItem> workflow = this.client.newWorkflowStub(NestingExecWorkflow.class, options);

    workflow.performWorkload(WFAddr.ROOT, workload, 0, maxBranchesPerTree, maxSubTreesPerTree, Optional.empty());
  }
}
