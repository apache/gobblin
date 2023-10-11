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

package org.apache.gobblin.temporal.ddm.launcher;

import io.temporal.client.WorkflowOptions;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.hadoop.fs.Path;

import static org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX;


/**
 * A {@link JobLauncher} for the initial triggering of a Temporal workflow that executes {@link WorkUnit}s to fulfill
 * the work they specify.  see: {@link ProcessWorkUnitsWorkflow}
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler#buildJobLauncher(Properties)} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
@Slf4j
public class ProcessWorkUnitsJobLauncher extends GobblinTemporalJobLauncher {
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_NAME_NODE_URI = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "name.node.uri";
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_UNITS_DIR = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "work.units.dir";

  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_BRANCHES_PER_TREE = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "work.max.branches.per.tree";
  public static String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_SUB_TREES_PER_TREE = GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX + "work.max.sub.trees.per.tree";

  public ProcessWorkUnitsJobLauncher(
      Properties jobProps,
      Path appWorkDir,
      List<? extends Tag<?>> metadataTags,
      ConcurrentHashMap<String, Boolean> runningMap
  ) throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap);
  }

  @Override
  public void submitJob(List<WorkUnit> workunits) {
    WorkflowOptions options = WorkflowOptions.newBuilder().setTaskQueue(this.queueName).build();
    ProcessWorkUnitsWorkflow workflow = this.client.newWorkflowStub(ProcessWorkUnitsWorkflow.class, options);
    try {
      URI nameNodeUri = new URI(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_NAME_NODE_URI, "<<not-set>>"));
      // NOTE: `Path` is challenging for temporal to ser/de, but still pre-construct as `Path`, to validate prop str contents
      Path workUnitsDir = new Path(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_UNITS_DIR, "<<not-set>>"));
      WUProcessingSpec wuSpec = new WUProcessingSpec(nameNodeUri, workUnitsDir.toString());
      if (this.jobProps.containsKey(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_BRANCHES_PER_TREE) &&
          this.jobProps.containsKey(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_SUB_TREES_PER_TREE)) {
        int maxBranchesPerTree = Integer.valueOf(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_BRANCHES_PER_TREE));
        int maxSubTreesPerTree = Integer.valueOf(this.jobProps.getProperty(GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_SUB_TREES_PER_TREE));
        wuSpec.setTuning(new WUProcessingSpec.Tuning(maxBranchesPerTree, maxSubTreesPerTree));
      }
      workflow.process(wuSpec);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
