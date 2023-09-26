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
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.jetbrains.annotations.NotNull;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.KillDagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.apache.gobblin.service.ExecutionStatus.ORCHESTRATED;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * Holds a stream of {@link DagTask} that {@link DagProcessingEngine} would pull from to process, as it is ready for more work.
 * It provides an implementation for {@link DagManagement} that defines the rules for a flow and job.
 * Implements {@link Iterable} to provide {@link DagTask}s as soon as it's available to {@link DagProcessingEngine}
 */

@Alpha
@Slf4j
@AllArgsConstructor
public class DagTaskStream implements Iterable<DagTask>, DagManagement {

  @Getter
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();
  private FlowTriggerHandler flowTriggerHandler;
  private DagManagementStateStore dagManagementStateStore;
  private DagManagerMetrics dagManagerMetrics;


  @NotNull
  @Override
  public Iterator<DagTask> iterator() {
    return new Iterator<DagTask>() {

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public DagTask next() {

        DagTask dagTask = null;
        while(true) {
          DagActionStore.DagAction dagAction = take();
          Properties jobProps = getJobProperties(dagAction);
          try {
            MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = flowTriggerHandler.getLeaseOnDagAction(jobProps, dagAction, System.currentTimeMillis());
            if(leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
              dagTask = createDagTask(dagAction,
                  (MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseAttemptStatus);
            }
            if (dagTask != null) {
              break; // Exit the loop when dagTask is non-null
            }
          } catch (IOException e) {
            //TODO: need to handle exceptions gracefully
            throw new RuntimeException(e);
          }
        }
        return dagTask;
      }
    };
  }

  private boolean add(DagActionStore.DagAction dagAction) {
    return this.dagActionQueue.offer(dagAction);
  }

  private DagActionStore.DagAction take() {
    return this.dagActionQueue.poll();
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus) {
    DagActionStore.FlowActionType flowActionType = dagAction.getFlowActionType();
    switch (flowActionType) {
      case KILL:
        return new KillDagTask(dagAction, leaseObtainedStatus);
      case RESUME:
      case LAUNCH:
      case ADVANCE:
      default:
        log.warn("It should not reach here. Yet to provide implementation.");
        return null;
    }
  }

  protected void complete(DagTask dagTask) throws IOException {
    dagTask.conclude(this.flowTriggerHandler.getMultiActiveLeaseArbiter());
  }

  @Override
  public void launchFlow(String flowGroup, String flowName, long eventTimestamp) {
    //TODO: provide implementation after finalizing code flow
    throw new UnsupportedOperationException("Currently launch flow is not supported.");
  }

  @Override
  public void resumeFlow(DagActionStore.DagAction killAction, long eventTimestamp) {
    //TODO: provide implementation after finalizing code flow
    throw new UnsupportedOperationException("Currently resume flow is not supported.");
  }

  @Override
  public void killFlow(DagActionStore.DagAction killAction, long eventTimestamp) throws IOException {
     if(!add(killAction)) {
        throw new IOException("Could not add kill dag action: " + killAction + " to the queue.");
      }
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
    //TODO: need to distribute the responsibility outside this class
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
    //TODO: initialize default job sla in millis via configs
    long timeOutForJobStart = DagManagerUtils.getJobStartSla(node, System.currentTimeMillis());
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

  protected JobStatus retrieveJobStatus(Dag.DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

    return getStatus(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
  }

  /**
   * Retrieve the flow's {@link JobStatus} (i.e. job status with {@link JobStatusRetriever#NA_KEY} as job name/group) from a dag
   */
  protected JobStatus retrieveFlowStatus(Dag<JobExecutionPlan> dag) {
    if (dag == null || dag.isEmpty()) {
      return null;
    }
    Config jobConfig = dag.getNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return getStatus(flowGroup, flowName, flowExecutionId, JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY);
  }

  private JobStatus getStatus(String flowGroup, String flowName, long flowExecutionId, String jobGroup, String jobName) {
    throw new UnsupportedOperationException("Currently retrieval of flow/job status is not supported");
  }

  private Properties getJobProperties(DagActionStore.DagAction dagAction) {
    String dagId = String.valueOf(
        DagManagerUtils.generateDagId(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId()));
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDagIdToDags().get(dagId);
    return dag.getStartNodes().get(0).getValue().getJobSpec().getConfigAsProperties();
  }

}
