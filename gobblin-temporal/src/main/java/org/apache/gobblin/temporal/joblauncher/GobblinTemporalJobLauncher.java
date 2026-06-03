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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowStub;
import io.temporal.workflow.Workflow;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.service.ManagedWorkflowServiceStubs;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys.*;
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
public abstract class GobblinTemporalJobLauncher extends GobblinJobLauncher {
  private static final Logger log = Workflow.getLogger(GobblinTemporalJobLauncher.class);
  private static final int TERMINATION_TIMEOUT_SECONDS = 3;

  protected ManagedWorkflowServiceStubs managedWorkflowServiceStubs;
  protected WorkflowClient client;
  protected String queueName;
  protected String namespace;
  protected String workflowId;

  // In temporal-on-yarn each AM JVM launches exactly one workflow (see {@link #handleLaunchFinalization}),
  // so a static field is the single source of terminal status for the AM. Populated by
  // {@link #handleLaunchFinalization} -> {@link #captureTerminalWorkflowStatus} (normal completion, while
  // Temporal stubs are still open). Read by {@code GobblinTemporalApplicationMaster.main()} after
  // {@code start()} returns to drive {@code System.exit}, and by the temporal {@code YarnService} to derive
  // the {@link FinalApplicationStatus} reported to YARN on un-register.
  private static volatile WorkflowExecutionStatus lastTerminalStatus = null;

  public GobblinTemporalJobLauncher(Properties jobProps, Path appWorkDir,
                                    List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap, EventBus eventBus)
          throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap, eventBus);
    log.info("GobblinTemporalJobLauncher: appWorkDir {}; jobProps {}", appWorkDir, jobProps);

    String connectionUri = jobProps.getProperty(TEMPORAL_CONNECTION_STRING);
    this.managedWorkflowServiceStubs = createServiceInstance(connectionUri);

    this.namespace = jobProps.getProperty(GOBBLIN_TEMPORAL_NAMESPACE, DEFAULT_GOBBLIN_TEMPORAL_NAMESPACE);
    this.client = createClientInstance(managedWorkflowServiceStubs.getWorkflowServiceStubs(), namespace);

    this.queueName = jobProps.getProperty(GOBBLIN_TEMPORAL_TASK_QUEUE, DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

    // non-null value indicates job has been submitted
    this.workflowId = null;
    // Reset the process-wide terminal-status cache for this fresh launcher. In a real AM JVM only one
    // launcher ever runs, but tests re-instantiate within the same JVM and must not see leaked status.
    setLastTerminalStatus(null);
    startCancellationExecutor();
  }

  /**
   * Query Temporal for the current execution status of {@link #workflowId}. Transient gRPC failures
   * (e.g. UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED) are already retried with backoff by the Temporal
   * service stubs' configured {@code RpcRetryOptions} (tunable via {@code gobblin.temporal.rpc.retry.options.*};
   * see {@code TemporalWorkflowClientFactory}), so this method intentionally adds no retry loop of its own.
   * Returns {@code WORKFLOW_EXECUTION_STATUS_UNSPECIFIED} as a safe fallback only if the describe still fails
   * (i.e. Temporal stayed unreachable beyond those configured retries), so callers downstream emit a JOB_FAILED
   * rather than swallowing the GTE entirely.
   */
  private WorkflowExecutionStatus fetchWorkflowStatus() {
    try {
      WorkflowStub workflowStub = this.client.newUntypedWorkflowStub(this.workflowId);
      DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
          .setNamespace(this.namespace)
          .setExecution(workflowStub.getExecution())
          .build();
      DescribeWorkflowExecutionResponse response = managedWorkflowServiceStubs.getWorkflowServiceStubs()
          .blockingStub().describeWorkflowExecution(request);
      return response.getWorkflowExecutionInfo().getStatus();
    } catch (Exception e) {
      log.warn("Failed to describe workflow {} for completion GTE even after the service stubs' configured RPC "
          + "retries; treating as UNSPECIFIED (will emit JOB_FAILED)", this.workflowId, e);
      return WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED;
    }
  }

  /**
   * Map a Temporal {@link WorkflowExecutionStatus} to the YARN {@link FinalApplicationStatus} the AM should
   * report when it un-registers, so a launcher polling the {@code ApplicationReport} sees the true outcome
   * instead of an unconditional SUCCEEDED. Kept in lockstep with {@link #computeExitCode}: COMPLETED -> SUCCEEDED,
   * CANCELED -> KILLED, everything else -> FAILED. A {@code null} status means the AM is going down without ever
   * having observed the workflow reach a terminal state (e.g. RM preemption / AMRM error mid-run, or a crash
   * before the job finished), so the job did NOT complete successfully -> FAILED.
   */
  @VisibleForTesting
  public static FinalApplicationStatus mapWorkflowStatusToFinalAppStatus(WorkflowExecutionStatus status) {
    if (status == null) {
      return FinalApplicationStatus.FAILED;
    }
    switch (status) {
      case WORKFLOW_EXECUTION_STATUS_COMPLETED:
        return FinalApplicationStatus.SUCCEEDED;
      case WORKFLOW_EXECUTION_STATUS_CANCELED:
        return FinalApplicationStatus.KILLED;
      case WORKFLOW_EXECUTION_STATUS_FAILED:
      case WORKFLOW_EXECUTION_STATUS_TERMINATED:
      case WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
      case WORKFLOW_EXECUTION_STATUS_RUNNING:
      case WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
      case WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return FinalApplicationStatus.FAILED;
    }
  }

  /** @return {@link Config} now featuring all overrides rooted at {@link GobblinTemporalConfigurationKeys#GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES} */
  protected Config applyJobLauncherOverrides(Config config) {
    Config configOverrides = ConfigUtils.getConfig(config,
        GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES, ConfigFactory.empty());
    log.info("appying config overrides: {}", configOverrides);
    return configOverrides.withFallback(config);
  }

  @Override
  protected void handleLaunchFinalization() {
    // NOTE: This code only makes sense when there is 1 source / workflow being launched per application for Temporal. This is a stop-gap
    // for achieving batch job behavior. Given the current constraints of yarn applications requiring a static proxy user
    // during application creation, it is not possible to have multiple workflows running in the same application.
    // and so it makes sense to just kill the job after this is completed
    // Capture the terminal workflow status now, while Temporal service stubs are guaranteed open. AM main()
    // reads this cache to set the JVM exit code, and the temporal YarnService reads it to derive the
    // FinalApplicationStatus reported to YARN on un-register.
    captureTerminalWorkflowStatus();
    log.info("Requesting the AM to shutdown after the job {} completed", this.jobContext.getJobId());
    eventBus.post(new ClusterManagerShutdownRequest());
  }

  @VisibleForTesting
  void captureTerminalWorkflowStatus() {
    if (this.workflowId == null) {
      return;
    }
    WorkflowExecutionStatus status = fetchWorkflowStatus();
    setLastTerminalStatus(status);
    log.info("Captured terminal workflow status {} for workflow {} (drives FinalApplicationStatus and AM exit code)",
        status, this.workflowId);
  }

  /**
   * @return the most recently captured terminal {@link WorkflowExecutionStatus} for the AM's single workflow,
   * or {@code null} if {@link #handleLaunchFinalization} never ran (e.g., AM crashed before any job was
   * submitted, or before the workflow reached a terminal state).
   */
  public static WorkflowExecutionStatus getLastTerminalStatus() {
    return lastTerminalStatus;
  }

  // Static mutator so the process-wide cache is written from a static context (the launcher-instance lifecycle
  // delegates here): avoids FindBugs ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD while preserving the single-source
  // bridge from the launcher instance to GobblinTemporalApplicationMaster.main() / the temporal YarnService.
  private static void setLastTerminalStatus(WorkflowExecutionStatus status) {
    lastTerminalStatus = status;
  }

  /**
   * Convert a {@link WorkflowExecutionStatus} into a process exit code: {@code 0} for a clean
   * {@code COMPLETED} workflow, {@code 1} for anything else (failed, cancelled, terminated, timed out,
   * still running at shutdown, unspecified/unknown, or {@code null} = no terminal status captured). Used by
   * {@code GobblinTemporalApplicationMaster.main()} to surface job-level failures as non-zero AM JVM exit codes.
   */
  public static int computeExitCode(WorkflowExecutionStatus status) {
    return status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED ? 0 : 1;
  }

  /**
   * Submit a job to run.
   */
  @Override
  abstract protected void submitJob(List<WorkUnit> workUnits) throws Exception;

  @Override
  protected void executeCancellation() {
    if (this.workflowId == null) {
      log.info("Cancellation of temporal workflow attempted without submitting it");
      return;
    }

    log.info("Cancelling temporal workflow {}", this.workflowId);
    try {
      WorkflowStub workflowStub = this.client.newUntypedWorkflowStub(this.workflowId);

      // Describe the workflow execution to get its status
      DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
          .setNamespace(this.namespace)
          .setExecution(workflowStub.getExecution())
          .build();
      DescribeWorkflowExecutionResponse response = managedWorkflowServiceStubs.getWorkflowServiceStubs()
          .blockingStub().describeWorkflowExecution(request);

      WorkflowExecutionStatus status;
      try {
        status = response.getWorkflowExecutionInfo().getStatus();
      } catch (Exception e) {
        log.warn("Exception occurred while getting status of the workflow " + this.workflowId
            + ". We would still attempt the cancellation", e);
        workflowStub.cancel();
        log.info("Temporal workflow {} cancelled successfully", this.workflowId);
        return;
      }

      // Check if the workflow is not finished
      if (status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED) {
        workflowStub.cancel();
        try {
          // Check workflow status, if it is cancelled, will throw WorkflowFailedException else TimeoutException
          workflowStub.getResult(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS, String.class, String.class);
        } catch (TimeoutException te) {
          // Workflow is still running, terminate it.
          log.info("Workflow is still running, will attempt termination", te);
          workflowStub.terminate("Job cancel invoked");
        } catch (WorkflowFailedException wfe) {
          // Do nothing as exception is expected.
        }
        log.info("Temporal workflow {} cancelled successfully", this.workflowId);
      } else {
        log.info("Workflow {} is already finished with status {}", this.workflowId, status);
      }
    } catch (Exception e) {
      log.error("Exception occurred while cancelling the workflow " + this.workflowId, e);
    }
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) {
    log.warn("NOT IMPLEMENTED: Temporal removeTasksFromCurrentJob");
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) {
    log.warn("NOT IMPLEMENTED: Temporal addTasksToCurrentJob");
  }

  @Override
  public void close() throws IOException {
    try {
      // Calling cancel before calling close on serviceStubs as it will shutdown the service which is required during cancellation.
      cancelJob(jobListener);
    } catch (Exception e) {
      log.error("Exception occurred while cancelling job", e);
    } finally {
      managedWorkflowServiceStubs.close();
      super.close();
    }
  }
}
