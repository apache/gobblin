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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
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
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;
  private static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer TERMINATION_TIMEOUT = 30;

  public static final String NUM_THREADS_KEY = "gobblin.service.dagManager.numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = "gobblin.service.dagManager.pollingInterval";
  public static final String JOB_STATUS_RETRIEVER_KEY = "gobblin.service.dagManager.jobStatusRetriever";
  public static final String DAG_STORE_CLASS_KEY = "gobblin.service.dagManager.dagStateStoreClass";
  public static final String DAG_STATESTORE_DIR = "gobblin.service.dagManager.dagStateStoreDir";

  private BlockingQueue<Dag<JobExecutionPlan>> queue;
  private ScheduledExecutorService scheduledExecutorPool;
  private boolean instrumentationEnabled = true;

  private final Integer numThreads;
  private final Integer pollingInterval;
  private final JobStatusRetriever jobStatusRetriever;
  private final DagStateStore dagStateStore;
  private volatile boolean isActive = false;

  public DagManager(Config config, boolean instrumentationEnabled) {
    this.queue = new LinkedBlockingDeque<>();
    this.numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);
    this.instrumentationEnabled = instrumentationEnabled;

    try {
      Class jobStatusRetrieverClass = Class.forName(config.getString(JOB_STATUS_RETRIEVER_KEY));
      this.jobStatusRetriever =
          (JobStatusRetriever) GobblinConstructorUtils.invokeLongestConstructor(jobStatusRetrieverClass);
      Class dagStateStoreClass = Class.forName(config.getString(DAG_STORE_CLASS_KEY));
      this.dagStateStore = (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Exception encountered during DagManager initialization", e);
    }
  }

  public DagManager(Config config) {
    this(config, true);
  }

  /** Start the service. On startup, the service launches a fixed pool of {@link DagManagerThread}s, which are scheduled at
   * fixed intervals. The service also loads any {@link Dag}s
   */
  @Override
  protected void startUp() {
    //On startup, the service creates tasks that are scheduled at a fixed rate.
    for (int i = 0; i < numThreads; i++) {
      this.scheduledExecutorPool.scheduleAtFixedRate(new DagManagerThread(jobStatusRetriever, dagStateStore, queue, instrumentationEnabled), 0, this.pollingInterval,
          TimeUnit.SECONDS);
    }
  }

  /**
   * Method to submit a {@link Dag} to the {@link DagManager}. The {@link DagManager} first persists the
   * submitted dag to the {@link DagStateStore} and then adds the dag to a {@link BlockingQueue} to be picked up
   * by one of the {@link DagManagerThread}s.
   */
  public synchronized void offer(Dag<JobExecutionPlan> dag) throws IOException {
    //Persist the dag
    this.dagStateStore.writeCheckpoint(dag);
    //Add it to the queue of dags
    if (!this.queue.offer(dag)) {
      throw new IOException("Could not add dag" + DagManagerUtils.generateDagId(dag) + "to queue");
    }
  }

  /**
   * When a {@link DagManager} becomes active, it loads the serialized representations of the currently running {@link Dag}s
   * from the checkpoint directory, deserializes the {@link Dag}s and adds them to a queue to be consumed by
   * the {@link DagManagerThread}s.
   * @param active a boolean to indicate if the {@link DagManager} is the leader.
   */
  public synchronized void setActive(boolean active) {
    this.isActive = active;
    try {
      if (this.isActive) {
        for (Dag<JobExecutionPlan> dag : dagStateStore.getDags()) {
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
  public static class DagManagerThread implements Runnable {
    private Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new HashMap<>();
    private Map<String, Dag<JobExecutionPlan>> dags = new HashMap<>();
    private Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new HashMap<>();
    private Set<String> failedDagIds = new HashSet<>();
    private JobStatusRetriever jobStatusRetriever;
    private DagStateStore dagStateStore;
    private BlockingQueue<Dag<JobExecutionPlan>> queue;
    private final MetricContext metricContext;
    private final Optional<EventSubmitter> eventSubmitter;

    /**
     * Constructor.
     */
    public DagManagerThread(JobStatusRetriever jobStatusRetriever, DagStateStore dagStateStore, BlockingQueue<Dag<JobExecutionPlan>> queue,
        boolean instrumentationEnaled) {
      this.jobStatusRetriever = jobStatusRetriever;
      this.dagStateStore = dagStateStore;
      this.queue = queue;
      if (instrumentationEnaled) {
        this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
        this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
      } else {
        this.metricContext = null;
        this.eventSubmitter = Optional.absent();
      }
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
        log.info("Polling job statuses..");
        TimingEvent jobStatusPollTimer = this.eventSubmitter.isPresent()
            ? eventSubmitter.get().getTimingEvent(TimingEvent.JobStatusTimings.ALL_JOB_STATUSES_POLLED)
            : null;
        //Poll and update the job statuses of running jobs.
        pollJobStatuses();
        log.info("Poll done.");
        if (jobStatusPollTimer != null) {
          jobStatusPollTimer.stop();
        }
        //Clean up any finished dags
        log.info("Cleaning up finished dags..");
        cleanUp();
        log.info("Clean up done");
      } catch (Exception e) {
        log.error("Exception encountered in {}", getClass().getName(), e);
      }
    }

    /**
     * This method determines the next set of jobs to execute from the dag and submits them for execution.
     * This method updates internal data structures tracking currently running Dags and jobs.
     */
    private void initialize(Dag<JobExecutionPlan> dag)
        throws IOException {
      //Add Dag to the map of running dags
      String dagId = DagManagerUtils.generateDagId(dag);
      log.info("Initializing Dag {}", dagId);
      if (this.dags.containsKey(dagId)) {
        log.warn("Already tracking a dag with dagId {}, skipping.", dagId);
        return;
      }

      this.dags.put(dagId, dag);
      log.info("Dag {} - determining if any jobs are already running.");
      //Are there any jobs already in the running state? This check is for Dags already running
      //before a leadership change occurs.
      for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        if (DagManagerUtils.getExecutionStatus(dagNode) == ExecutionStatus.RUNNING) {
          addJobState(dagId, dagNode);
        }
      }
      log.info("Dag {} submitting jobs ready for execution.");
      //Determine the next set of jobs to run and submit them for execution
      submitNext(dagId);
      log.info("Dag {} Initialization complete.");
    }

    /**
     * Poll the statuses of running jobs.
     * @return List of {@link JobStatus}es.
     */
    private void pollJobStatuses()
        throws IOException {
      this.failedDagIds = new HashSet<>();
      for (Dag.DagNode<JobExecutionPlan> node : this.jobToDag.keySet()) {
        TimingEvent jobStatusPollTimer = this.eventSubmitter.isPresent()
            ? eventSubmitter.get().getTimingEvent(TimingEvent.JobStatusTimings.JOB_STATUS_POLLED)
            : null;
        JobStatus jobStatus = pollJobStatus(node);
        if (jobStatusPollTimer != null) {
          jobStatusPollTimer.stop();
        }
        Preconditions.checkNotNull(jobStatus, "Received null job status for a running job " + DagManagerUtils.getJobName(node));
        JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(node);
        //TODO: This will be updated when JobStatus schema provides the correct execution status.
        //Currently, it is a placeholder.
        switch (jobStatus.getEventName()) {
          case TimingEvent.LauncherTimings.JOB_COMPLETE:
            jobExecutionPlan.setExecutionStatus(ExecutionStatus.COMPLETE);
            onJobFinish(node);
            break;
          case TimingEvent.LauncherTimings.JOB_FAILED:
            jobExecutionPlan.setExecutionStatus(ExecutionStatus.FAILED);
            onJobFinish(node);
            break;
          default:
            jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
            break;
        }
      }
    }

    /**
     * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
     */
    private JobStatus pollJobStatus(Dag.DagNode<JobExecutionPlan> dagNode) {
      Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
      String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
      long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

      Iterator<JobStatus> jobStatusIterator =
          this.jobStatusRetriever.getJobStatusForFlowExecution(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
      if (jobStatusIterator.hasNext()) {
        return jobStatusIterator.next();
      } else {
        return null;
      }
    }

    public void submitNext(String dagId)
        throws IOException {
      Dag<JobExecutionPlan> dag = this.dags.get(dagId);
      //Submit jobs from the dag ready for execution.
      for (Dag.DagNode<JobExecutionPlan> dagNode : DagManagerUtils.getNext(dag)) {
        submitJob(dagNode);
        addJobState(dagId, dagNode);
      }
      //Checkpoint the dag state
      this.dagStateStore.writeCheckpoint(dag);
    }

    /**
     * Submits a {@link JobSpec} to a {@link org.apache.gobblin.runtime.api.SpecExecutor}.
     */
    private void submitJob(Dag.DagNode<JobExecutionPlan> dagNode) {
      JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
      JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);

      // Run this spec on selected executor
      SpecProducer producer = null;
      try {
        producer = DagManagerUtils.getSpecProducer(dagNode);
        Config jobConfig = DagManagerUtils.getJobConfig(dagNode);
        if (!jobConfig.hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
          log.warn("JobSpec does not contain flowExecutionId.");
        }
        log.info("Submitting job: {} on executor: {}", jobSpec, producer);
        producer.addSpec(jobSpec);
      } catch (Exception e) {
        log.error("Cannot submit job: {} on executor: {}", jobSpec, producer, e);
      }
    }

    /**
     * Method that defines the actions to be performed when a job finishes either successfully or with failure.
     * This method updates the state of the dag and performs clean up actions as necessary.
     */
    private void onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode)
        throws IOException {
      Dag<JobExecutionPlan> dag = this.jobToDag.get(dagNode);
      String dagId = DagManagerUtils.generateDagId(dag);
      String jobName = DagManagerUtils.getJobName(dagNode);
      ExecutionStatus jobStatus = DagManagerUtils.getExecutionStatus(dagNode);
      log.info("Job {} of Dag {} has finished with status {}", jobName, dagId, jobStatus.name());

      deleteJobState(dagId, dagNode);

      if (jobStatus == ExecutionStatus.COMPLETE) {
        submitNext(dagId);
      } else if (jobStatus == ExecutionStatus.FAILED) {
        this.failedDagIds.add(dagId);
      }
    }

    private void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
      this.jobToDag.remove(dagNode);
      this.dagToJobs.get(dagId).remove(dagNode);
    }

    private void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
      Dag<JobExecutionPlan> dag = this.dags.get(dagId);
      this.jobToDag.put(dagNode, dag);
      if (this.dagToJobs.containsKey(dagId)) {
        this.dagToJobs.get(dagId).add(dagNode);
      } else {
        LinkedList<Dag.DagNode<JobExecutionPlan>> dagNodeList = Lists.newLinkedList();
        dagNodeList.add(dagNode);
        this.dagToJobs.put(dagId, dagNodeList);
      }
    }

    private boolean hasRunningJobs(String dagId) {
      return !this.dagToJobs.get(dagId).isEmpty();
    }

    /**
     * Perform clean up. Remove a dag from the dagstore if the dag is complete and update internal state.
     */
    private void cleanUp() {
      //Clean up failed dags
      for (String dagId : this.failedDagIds) {
        //Skip monitoring of any other jobs of the failed dag.
        LinkedList<Dag.DagNode<JobExecutionPlan>> dagNodeList = this.dagToJobs.get(dagId);
        while (!dagNodeList.isEmpty()) {
          Dag.DagNode<JobExecutionPlan> dagNode = dagNodeList.poll();
          deleteJobState(dagId, dagNode);
        }
        log.info("Dag {} has finished with status FAILED; Cleaning up dag from the state store.", dagId);
        cleanUpDag(dagId);
      }

      //Clean up successfully completed dags
      for (String dagId : this.dags.keySet()) {
        if (!hasRunningJobs(dagId)) {
          log.info("Dag {} has finished with status COMPLETE; Cleaning up dag from the state store.", dagId);
          cleanUpDag(dagId);
        }
      }
    }

    private void cleanUpDag(String dagId) {
      Dag<JobExecutionPlan> dag = this.dags.get(dagId);
      this.dagToJobs.remove(dagId);
      this.dags.remove(dagId);
      this.dagStateStore.cleanUp(dag);
    }
  }

  /** Stop the service. */
  @Override
  protected void shutDown()
      throws Exception {
    this.scheduledExecutorPool.shutdown();
    this.scheduledExecutorPool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
  }
}
