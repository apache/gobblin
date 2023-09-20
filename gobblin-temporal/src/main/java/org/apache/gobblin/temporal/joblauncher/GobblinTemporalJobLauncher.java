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

package org.apache.gobblin.temporal.joblauncher;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.IllustrationTask;
import org.apache.gobblin.temporal.workflows.NestingExecWorkflow;
import org.apache.gobblin.temporal.workflows.SimpleGeneratedWorkload;
import org.apache.gobblin.temporal.workflows.WFAddr;
import org.apache.gobblin.temporal.workflows.Workload;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.SerializationUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createClientInstance;
import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createServiceInstance;

/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Temporal task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the gobblin
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
@Slf4j
public class GobblinTemporalJobLauncher extends GobblinJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalJobLauncher.class);

  private WorkflowServiceStubs workflowServiceStubs;
  private WorkflowClient client;

  public GobblinTemporalJobLauncher(Properties jobProps, Path appWorkDir,
                                    List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap)
          throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap);
    LOGGER.debug("GobblinTemporalJobLauncher: jobProps {}, appWorkDir {}", jobProps, appWorkDir);
    this.workflowServiceStubs = createServiceInstance();
    this.client = createClientInstance(workflowServiceStubs);

    /*
     * Set Workflow options such as WorkflowId and Task Queue so the worker knows where to list and which workflows to execute.
     */
    startCancellationExecutor();
  }

  /**
   * Submit a job to run.
   */
  @Override
  protected void submitJob(List<WorkUnit> workUnits) throws Exception{
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      Path jobStateFilePath;

      // write the job.state using the state store if present, otherwise serialize directly to the file
      if (this.stateStores.haveJobStateStore()) {
        jobStateFilePath = GobblinClusterUtils.getJobStateFilePath(true, this.appWorkDir, this.jobContext.getJobId());
        this.stateStores.getJobStateStore()
                .put(jobStateFilePath.getParent().getName(), jobStateFilePath.getName(), this.jobContext.getJobState());
      } else {
        jobStateFilePath = GobblinClusterUtils.getJobStateFilePath(false, this.appWorkDir, this.jobContext.getJobId());
        SerializationUtils.serializeState(this.fs, jobStateFilePath, this.jobContext.getJobState());
      }

      // Block on persistence of all workunits to be finished.
      stateSerDeRunner.waitForTasks(Long.MAX_VALUE);

      LOGGER.debug("GobblinTemporalJobLauncher.createTemporalJob: jobStateFilePath {}, jobState {} jobProperties {}",
              jobStateFilePath, this.jobContext.getJobState().toString(), this.jobContext.getJobState().getProperties());

      String jobStateFilePathStr = jobStateFilePath.toString();

      List<CompletableFuture<Void>> futures = new ArrayList<>();

      int numTasks = PropertiesUtils.getPropAsInt(this.jobProps, "temporal.task.size", 100);
      int maxBranchesPerTree = PropertiesUtils.getPropAsInt(this.jobProps, "temporal.task.maxBranchesPerTree", 20);
      int maxSubTreesPerTree = PropertiesUtils.getPropAsInt(this.jobProps, "temporal.task.maxSubTreesPerTree", 5);
      ExecutorService executor = Executors.newFixedThreadPool(1);
      futures.add(CompletableFuture.runAsync(() -> {
        try {
          Workload<IllustrationTask> workload = SimpleGeneratedWorkload.createAs(numTasks);
          // WARNING: although type param must agree w/ that of `workload`, it's entirely unverified by type checker!
          // ...and more to the point, mismatch would occur at runtime (`performWork` on whichever workflow underpins stub)!
          WorkflowOptions options = WorkflowOptions.newBuilder().setTaskQueue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE).build();
          NestingExecWorkflow<IllustrationTask> workflow =
                  this.client.newWorkflowStub(NestingExecWorkflow.class, options);
          workflow.performWork(WFAddr.ROOT, workload, 0, maxBranchesPerTree, maxSubTreesPerTree, Optional.empty());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, executor));
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
  }

  @Override
  protected void executeCancellation() {
    LOGGER.info("Cancel temporal workflow");
  }

  @Override
  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) {
    LOGGER.info("Temporal removeTasksFromCurrentJob");
  }

  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) {
    LOGGER.info("Temporal addTasksToCurrentJob");
  }
}
