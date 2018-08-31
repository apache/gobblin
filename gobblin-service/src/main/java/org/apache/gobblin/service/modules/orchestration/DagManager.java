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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * This class implements a manager to manage the life cycle of a {@link Dag}. A {@link Dag} is submitted to the
 * {@link DagManager} by the {@link Orchestrator#orchestrate(Spec)} method. On receiving a {@link Dag}, the
 * {@link DagManager} first persists the {@link Dag} to the {@link DagStateStore}, and then submits it to a {@link BlockingQueue}.
 * This guarantees that each {@link Dag} received by the {@link DagManager} can be recovered in case of a leadership
 * change or service restart.
 *
 * The implementation of the {@link DagManager} is multi-threaded. Each {@link DagManagerThread} polls the
 * {@link BlockingQueue} for new Dag submissions at fixed intervals. It deques any newly submitted Dags and coordinates
 * the execution of individual jobs in the Dag. The coordination logic involves polling the {@link JobStatus}es of running
 * jobs. Upon completion of a job, it will either schedule the next job in the Dag (on SUCCESS) or mark the Dag as failed
 * (on FAILURE). Upon completion of a Dag execution, it will perform the required clean up actions.
 *
 * The {@link DagManager} is active only in the leader mode. To ensure, each {@link Dag} managed by a {@link DagManager} is
 * checkpointed to a persistent location. On start up or leadership change,
 * the {@link DagManager} loads all the checkpointed {@link Dag}s and adds them to the {@link  BlockingQueue}.
 * Current implementation supports only FileSystem-based checkpointing of the Dag statuses.
 */
@Alpha
@Slf4j
public class DagManager extends AbstractIdleService {
  private static final String WORKFLOW_MANAGER_NUM_THREADS_KEY = "gobblin.service.workflow.manager.num.threads";
  private static final Integer DEFAULT_WORKFLOW_MANAGER_NUM_THREADS = 3;
  private static final String JOB_STATUS_POLLING_INTERVAL_KEY = "gobblin.service.workflow.manager.polling.interval";
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;
  private static final Integer TERMINATION_TIMEOUT = 30;
  private static final String JOB_STATUS_RETRIEVER_KEY = "gobblin.service.workflow.manager.job.status.retriever";
  private static final String DAG_STORE_CLASS_KEY = "gobblin.service.workflow.manager.dag.state.store.class";

  private BlockingQueue<Dag<JobExecutionPlan>> queue;
  private ScheduledExecutorService scheduledExecutorPool;

  private final Integer numThreads;
  private final Integer pollingInterval;
  private final JobStatusRetriever jobStatusRetriever;
  private final DagStateStore dagStateStore;
  private final MetricContext metricContext;
  private final Optional<EventSubmitter> eventSubmitter;
  private volatile boolean isActive = false;


  public DagManager(Config config, boolean instrumentationEnabled) throws ReflectiveOperationException {
    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), getClass());
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    }
    else {
      this.metricContext = null;
      this.eventSubmitter = Optional.absent();
    }

    this.queue = new LinkedBlockingDeque<>();
    this.numThreads = ConfigUtils.getInt(config,
        WORKFLOW_MANAGER_NUM_THREADS_KEY, DEFAULT_WORKFLOW_MANAGER_NUM_THREADS);
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);


    Class jobStatusRetrieverClass = Class.forName(config.getString(JOB_STATUS_RETRIEVER_KEY));
    this.jobStatusRetriever = (JobStatusRetriever) GobblinConstructorUtils.invokeLongestConstructor(jobStatusRetrieverClass, config);
    Class dagStateStoreClass = Class.forName(config.getString(DAG_STORE_CLASS_KEY));
    this.dagStateStore = (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config);
  }

  public DagManager(Config config) throws ReflectiveOperationException {
    this(config, true);
  }

  /** Start the service. On startup, the service launches a fixed pool of {@link DagManagerThread}s, which are scheduled at
   * fixed intervals. The service also loads any {@link Dag}s
   */
  @Override
  protected void startUp() throws Exception {
    //On startup, the service creates tasks that are scheduled at a fixed rate.
    for (int i = 0; i < numThreads; i++) {
      this.scheduledExecutorPool.scheduleAtFixedRate(new DagManagerThread(jobStatusRetriever, dagStateStore), 0, this.pollingInterval,
          TimeUnit.SECONDS);
    }
  }

  /**
   * Method to submit a {@link Dag} to the {@link DagManager}. The {@link DagManager} first persists the
   * submitted dag to the {@link DagStateStore} and then adds the dag to a {@link BlockingQueue} to be picked up
   * by one of the {@link DagManagerThread}s.
   * @param dag
   * @throws IOException
   */
  public void offer(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.writeCheckpoint(dag);
    if (!this.queue.offer(dag)) {
      throw new IOException("Could not add dag" + this.dagStateStore.generateDagId(dag) + "to queue");
    }
  }

  /**
   * When a {@link DagManager} becomes active, it loads the serialized representations of the currently running {@link Dag}s
   * from the checkpoint directory, deserializes the {@link Dag}s and adds them to a queue to be consumed by
   * the {@link DagManagerThread}s.
   * @param active a boolean to indicate if the {@link DagManager} is the leader.
   * @throws IOException
   */
  public synchronized void setActive(boolean active) {
    this.isActive = active;
    try {
      if (this.isActive) {
        for (Dag dag : dagStateStore.getRunningDags()) {
          offer(dag);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception encountered when activating the new DagManager", e);
    }
  }

  /**
   * Each {@link DagManagerThread} performs 2 actions when scheduled:
   * <ol>
   *   <li> Dequeues any newly submitted {@link Dag}s from the Dag queue. All the {@link JobExecutionPlan}s which
   *   are part of the dequed {@link Dag} will be managed this thread. </li>
   *   <li> Polls the job status store for the current job statuses of all the running jobs it manages.</li>
   * </ol>
   */
  public class DagManagerThread implements Runnable {
    private Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> runningJobs = new HashMap<>();
    private Map<String, Dag<JobExecutionPlan>> runningDags = new HashMap<>();
    private JobStatusRetriever jobStatusRetriever;
    private DagStateStore dagStateStore;

    /**
     * Constructor.
     * @param jobStatusRetriever
     */
    public DagManagerThread(JobStatusRetriever jobStatusRetriever, DagStateStore dagStateStore) {
      this.jobStatusRetriever = jobStatusRetriever;
      this.dagStateStore = dagStateStore;
    }

    /**
     * Main body of the {@link DagManagerThread}.
     */
    @Override
    public void run() {
      try {
        Object nextItem = queue.poll();
        //Poll the queue for a new Dag to execute.
        if (nextItem != null) {
          Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) nextItem;

          //Initialize dag.
          initialize(dag);
        }
        //Poll and update the job statuses of running jobs.
        pollJobStatuses();
      } catch (Exception e) {
        log.error("Exception encountered in {}", getClass().getName(), e);
      }
    }

    /**
     * Traverse the dag to determine the next set of nodes to be executed. It starts with the startNodes of the dag and
     * identifies each node yet to be executed and for which each of its parent nodes is in the {@link ExecutionStatus#COMPLETE}
     * state.
     * @param dag
     * @return
     */
    private List<Dag.DagNode<JobExecutionPlan>> getNext(Dag<JobExecutionPlan> dag) {
      List<Dag.DagNode<JobExecutionPlan>> nextNodes = new ArrayList<>();
      List<Dag.DagNode<JobExecutionPlan>> nodesToExpand = Lists.newArrayList();
      nodesToExpand.addAll(dag.getStartNodes());
      for (Dag.DagNode<JobExecutionPlan> node: nodesToExpand) {
        if (node.getValue().getExecutionStatus() == ExecutionStatus.$UNKNOWN) {
          List<Dag.DagNode<JobExecutionPlan>> parentNodes = node.getParentNodes();
          for (Dag.DagNode<JobExecutionPlan> parentNode: parentNodes) {
            if (parentNode.getValue().getExecutionStatus() != ExecutionStatus.COMPLETE) {
              continue;
            }
          }
          nextNodes.add(node);
        } else if (node.getValue().getExecutionStatus() == ExecutionStatus.COMPLETE) {
          nodesToExpand.add(node);
        } else {
          return new ArrayList<>();
        }
      }
      return nextNodes;
    }

    /**
     * This method determines the next set of jobs to execute from the dag and submits them for execution.
     * This method updates internal data structures tracking currently running Dags and jobs.
     * @param dag
     * @throws IOException
     */
    private void initialize(Dag<JobExecutionPlan> dag) throws IOException {
      //Add Dag to the map of running dags
      String dagId = this.dagStateStore.generateDagId(dag);
      if (this.runningDags.containsKey(dagId)) {
        log.warn("Already tracking a dag with dagId {}, skipping.", dagId);
        return;
      }

      this.runningDags.put(dagId, dag);

      //Are there any jobs already in the running state? This check is for Dags already running
      //before a leadership change occurs.
      for(Dag.DagNode<JobExecutionPlan> dagNode: dag.getNodes()) {
        if (dagNode.getValue().getExecutionStatus() == ExecutionStatus.RUNNING) {
          this.runningJobs.put(dagNode, dag);
        }
      }

      //Determine the next set of jobs to run
      for (Dag.DagNode<JobExecutionPlan> dagNode : getNext(dag)) {
        submitJob(dagNode, dag);
      }

      //Checkpoint the dag state
      this.dagStateStore.writeCheckpoint(dag);
    }

    /**
     * A helper method to submit a job for execution and update internal state.
     * @param dagNode
     * @param dag
     */
    private void submitJob(Dag.DagNode<JobExecutionPlan> dagNode, Dag<JobExecutionPlan> dag) {
      JobExecutionPlan plan = dagNode.getValue();
      plan.setExecutionStatus(ExecutionStatus.RUNNING);
      submitJobToSpecExecutor(plan);
      //Add the jobs to the list of running jobs to be monitored.
      this.runningJobs.put(dagNode, dag);
    }

    /**
     * Submits a {@link JobSpec} to a {@link org.apache.gobblin.runtime.api.SpecExecutor}.
     * @param jobExecutionPlan
     */
    private void submitJobToSpecExecutor(JobExecutionPlan jobExecutionPlan) {
      // Run this spec on selected executor
      SpecProducer producer = null;
      try {
        producer = jobExecutionPlan.getSpecExecutor().getProducer().get();
        JobSpec jobSpec = jobExecutionPlan.getJobSpec();

        if (!jobSpec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
          log.warn("JobSpec does not contain flowExecutionId.");
        }
        producer.addSpec(jobSpec);
      } catch (Exception e) {
        log.error("Cannot submit job: {}, on executor: ", jobExecutionPlan.getJobSpec(), producer, e);
      }
    }

    /**
     * Poll the statuses of running jobs.
     * @return List of {@link JobStatus}es.
     */
    private void pollJobStatuses() throws IOException {
      for (Dag.DagNode<JobExecutionPlan> node : this.runningJobs.keySet()) {
        JobExecutionPlan plan = node.getValue();
        JobStatus jobStatus = getJobStatus(plan);

        //TODO: This will be updated when JobStatus schema provides the correct execution status.
        //Currently, it is a placeholder.
        switch (jobStatus.getMessage()) {
          case "COMPLETE":
            plan.setExecutionStatus(ExecutionStatus.COMPLETE);
            onJobFinish(node);
            break;
          case "FAILED":
            plan.setExecutionStatus(ExecutionStatus.FAILED);
            onJobFinish(node);
            break;
          case "RUNNING":
            plan.setExecutionStatus(ExecutionStatus.RUNNING);
            break;
          default:
            plan.setExecutionStatus(ExecutionStatus.$UNKNOWN);
            break;
        }
      }
    }

    /**
     * Method that defines the actions to be performed when a job finishes either successfully or with failure.
     * This method updates the state of the dag and performs clean up actions as necessary.
     * @param dagNode
     * @throws IOException
     */
    private void onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
      JobExecutionPlan plan = dagNode.getValue();
      if (plan.getExecutionStatus() == ExecutionStatus.COMPLETE) {
        Dag<JobExecutionPlan> dag = this.runningJobs.get(dagNode);
        List<Dag.DagNode<JobExecutionPlan>> nextNodes = getNext(dag);
        if (!nextNodes.isEmpty()) {
          this.dagStateStore.writeCheckpoint(dag);
          //Get next set of jobs to execute
          for (Dag.DagNode<JobExecutionPlan> nextNode : nextNodes) {
            submitJob(nextNode, dag);
          }
        }
      }
      cleanUp(dagNode);
    }

    private boolean isJobFinished(JobExecutionPlan jobExecutionPlan) {
      return jobExecutionPlan.getExecutionStatus() == ExecutionStatus.COMPLETE ||
          jobExecutionPlan.getExecutionStatus() == ExecutionStatus.FAILED;
    }

    /**
     * Clean up
     * @param dagNode
     */
    private void cleanUp(Dag.DagNode<JobExecutionPlan> dagNode) {
      Dag<JobExecutionPlan> dag = this.runningJobs.get(dagNode);
      this.runningJobs.remove(dagNode);

      //Check if any other node in the dag is not finished
      for (Dag.DagNode<JobExecutionPlan> node: dag.getNodes()) {
        if (!isJobFinished(node.getValue()))
          return;
      }

      this.runningDags.remove(this.dagStateStore.generateDagId(dag));
      this.dagStateStore.cleanUp(dag);
    }

    /**
     * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
     * @param jobExecutionPlan
     * @return {@link JobStatus}.
     */
    private JobStatus getJobStatus(JobExecutionPlan jobExecutionPlan) {
      Config jobConfig = jobExecutionPlan.getJobSpec().getConfig();
      String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
      Long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

      return this.jobStatusRetriever.getJobStatusForFlowExecution(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
    }
  }

  /** Stop the service. */
  @Override
  protected void shutDown() throws Exception {
    this.scheduledExecutorPool.shutdown();
    this.scheduledExecutorPool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
  }
}
