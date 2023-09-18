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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.apache.gobblin.service.ExecutionStatus.ORCHESTRATED;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * Holds a stream of {@link DagTask} that {@link DagManager} would pull from to process, as it is ready for more work.
 * It provides an implementation for {@link DagManagement} defines the rules for a flow and job.
 * Implements {@link Iterator} to provide the next {@link DagTask} if available to {@link DagManager}
 */

@Alpha
@Slf4j
public class DagTaskStream implements Iterator<Optional<DagTask>>, DagManagement {
  @Getter
  private final BlockingDeque<DagTask> taskQueue = new LinkedBlockingDeque<>();
  private JobStatusRetriever jobStatusRetriever;
  private Optional<Timer> jobStatusPolledTimer;

  private DagManagerMetrics dagManagerMetrics;

  private DagManagementStateStore dagManagementStateStore;

  private Long defaultJobStartSlaTimeMillis;
  private FlowTriggerHandler flowTriggerHandler;

  private Optional<DagActionStore> dagActionStore;
  private DagStateStore failedDagStateStore;
  private FlowCompilationValidationHelper flowCompilationValidationHelper;
  private FlowCatalog flowCatalog = new FlowCatalog(ConfigBuilder.create().build());

  //TODO: add ctor for instantiating the attributes (will be handled in the subsequent PR)


  @Override
  public boolean hasNext() {
    return true;
  }


  @Override
  public Optional<DagTask> next() {

    DagTask dagTask = taskQueue.peek();

    try {
      if(flowTriggerHandler.attemptDagTaskLeaseAcquisition(dagTask)) {
        return Optional.of(taskQueue.poll());
      }
    } catch (IOException e) {
      //TODO: need to handle exceptions gracefully
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  public void addDagTaskToStream(DagTask dagTask) throws InterruptedException {
    this.taskQueue.put(dagTask);
  }

  @Override
  public void launchFlow(String flowGroup, String flowName, long  triggerTimeStamp) {
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      FlowSpec spec = (FlowSpec) flowCatalog.getSpecs(flowUri);
      Optional<Dag<JobExecutionPlan>> optionalJobExecutionPlanDag =
          this.flowCompilationValidationHelper.createExecutionPlanIfValid(spec).toJavaUtil();
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata((FlowSpec) spec);
      FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata, optionalJobExecutionPlanDag.get());
      String flowExecutionId = flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD);
      LaunchDagTask launchDagTask = new LaunchDagTask(flowGroup, flowName, flowExecutionId);
      launchDagTask.initialize(optionalJobExecutionPlanDag.get().getNodes(), triggerTimeStamp);
      addDagTaskToStream(launchDagTask);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for flowId {} due to exception {}", flowId, e.getMessage());
    } catch (SpecNotFoundException e) {
      log.warn("Spec not found for flowId {} due to exception {}", flowId, e.getMessage());
    } catch (IOException e) {
      log.warn("Failed to add Job Execution Plan for flowId {} OR delete dag action from dagActionStore (check "
          + "stacktrace) due to exception {}", flowId, e.getMessage());
    } catch (InterruptedException e) {
      log.warn("SpecCompiler failed to reach healthy state before compilation of flowId {}. Exception: ", flowId, e);
    }
  }

  @Override
  public void resumeFlow(String flowGroup, String flowName, String flowExecutionId, long  triggerTimeStamp) throws IOException, InterruptedException {
    DagManager.DagId dagId = DagManagerUtils.generateDagId(flowGroup, flowName, flowExecutionId);
    Dag<JobExecutionPlan> dag = this.failedDagStateStore.getDag(dagId.toString());
    if (dag == null) {
      log.error("Dag " + dagId + " was found in memory but not found in failed dag state store");
      return;
    }
    ResumeDagTask resumeDagTask = new ResumeDagTask(dagId);
    resumeDagTask.initialize(dag, triggerTimeStamp);
    addDagTaskToStream(resumeDagTask);
  }

  @Override
  public void killFlow(String flowGroup, String flowName, String flowExecutionId, long  triggerTimeStamp) throws InterruptedException {
    DagManager.DagId dagId = DagManagerUtils.generateDagId(flowGroup, flowName, flowExecutionId);
    if(!this.dagManagementStateStore.getDagIdToDags().containsKey(dagId.toString())) {
      log.info("Invalid dag since not present in map. Hence cannot cancel it");
      return;
    }
    Dag<JobExecutionPlan> killDag = this.dagManagementStateStore.getDagIdToDags().get(dagId.toString());
    KillDagTask killDagTask = new KillDagTask(dagId);
    killDagTask.initialize(killDag, triggerTimeStamp);
    addDagTaskToStream(killDagTask);

  }
  /**
   * Check if the SLA is configured for the flow this job belongs to.
   * If it is, this method will try to cancel the job when SLA is reached.
   *
   * @param node dag node of the job
   * @return true if the job is killed because it reached sla
   * @throws ExecutionException exception
   * @throws InterruptedException exception
   */

  @Override
  public boolean enforceFlowCompletionDeadline(Dag.DagNode<JobExecutionPlan> node) throws ExecutionException, InterruptedException {
    //TODO: need to distribute the responsibility outside of this class
    long flowStartTime = DagManagerUtils.getFlowStartTime(node);
    long currentTime = System.currentTimeMillis();
    String dagId = DagManagerUtils.generateDagId(node).toString();

    long flowSla;
    if (this.dagManagementStateStore.getDagToSLA().containsKey(dagId)) {
      flowSla = this.dagManagementStateStore.getDagToSLA().get(dagId);
    } else {
      try {
        flowSla = DagManagerUtils.getFlowSLA(node);
      } catch (ConfigException e) {
        log.warn("Flow SLA for flowGroup: {}, flowName: {} is given in invalid format, using default SLA of {}",
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY),
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY),
            DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS);
        flowSla = DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS;
      }
      this.dagManagementStateStore.getDagToSLA().put(dagId, flowSla);
    }

    if (currentTime > flowStartTime + flowSla) {
      log.info("Flow {} exceeded the SLA of {} ms. Killing the job {} now...",
          node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), flowSla,
          node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY));
      dagManagerMetrics.incrementExecutorSlaExceeded(node);
      KillDagProc.killDagNode(node);

      this.dagManagementStateStore.getDagIdToDags().get(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_RUN_DEADLINE_EXCEEDED);
      this.dagManagementStateStore.getDagIdToDags().get(dagId).setMessage("Flow killed due to exceeding SLA of " + flowSla + " ms");

      return true;
    }
    return false;
  }

  /**
   * Cancel the job if the job has been "orphaned". A job is orphaned if has been in ORCHESTRATED
   * {@link ExecutionStatus} for some specific amount of time.
   * @param node {@link Dag.DagNode} representing the job
   * @param jobStatus current {@link JobStatus} of the job
   * @return true if the total time that the job remains in the ORCHESTRATED state exceeds
   * {@value ConfigurationKeys#GOBBLIN_JOB_START_SLA_TIME}.
   */

  @Override
  public boolean enforceJobStartDeadline(Dag.DagNode<JobExecutionPlan> node, JobStatus jobStatus) throws ExecutionException, InterruptedException {
    //TODO: need to distribute the responsibility outside of this class
    if (jobStatus == null) {
      return false;
    }
    ExecutionStatus executionStatus = valueOf(jobStatus.getEventName());
    long timeOutForJobStart = DagManagerUtils.getJobStartSla(node, this.defaultJobStartSlaTimeMillis);
    long jobOrchestratedTime = jobStatus.getOrchestratedTime();
    if (executionStatus == ORCHESTRATED && System.currentTimeMillis() - jobOrchestratedTime > timeOutForJobStart) {
      log.info("Job {} of flow {} exceeded the job start SLA of {} ms. Killing the job now...",
          DagManagerUtils.getJobName(node),
          DagManagerUtils.getFullyQualifiedDagName(node),
          timeOutForJobStart);
      dagManagerMetrics.incrementCountsStartSlaExceeded(node);
      KillDagProc.killDagNode(node);

      String dagId = DagManagerUtils.generateDagId(node).toString();
      dagManagementStateStore.getDagIdToDags().get(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_START_DEADLINE_EXCEEDED);
      dagManagementStateStore.getDagIdToDags().get(dagId).setMessage("Flow killed because no update received for " + timeOutForJobStart + " ms after orchestration");
      return true;
    } else {
      return false;
    }

  }

  /**
   * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
   */

  protected JobStatus pollJobStatus(Dag.DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

    return pollStatus(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
  }

  /**
   * Retrieve the flow's {@link JobStatus} (i.e. job status with {@link JobStatusRetriever#NA_KEY} as job name/group) from a dag
   */
  protected JobStatus pollFlowStatus(Dag<JobExecutionPlan> dag) {
    if (dag == null || dag.isEmpty()) {
      return null;
    }
    Config jobConfig = dag.getNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return pollStatus(flowGroup, flowName, flowExecutionId, JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY);
  }

  protected JobStatus pollStatus(String flowGroup, String flowName, long flowExecutionId, String jobGroup, String jobName) {
    long pollStartTime = System.nanoTime();
    Iterator<JobStatus> jobStatusIterator =
        this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, jobName, jobGroup);
//    Instrumented.updateTimer(this.jobStatusPolledTimer, System.nanoTime() - pollStartTime, TimeUnit.NANOSECONDS);

    if (jobStatusIterator.hasNext()) {
      return jobStatusIterator.next();
    } else {
      return null;
    }
  }
}
