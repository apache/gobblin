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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceRequester;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitorFactory;
import org.apache.gobblin.service.monitoring.KillFlowEvent;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


/**
 * This class implements a manager to manage the life cycle of a {@link Dag}. A {@link Dag} is submitted to the
 * {@link DagManager} by the {@link Orchestrator#orchestrate(Spec)} method. On receiving a {@link Dag}, the
 * {@link DagManager} first persists the {@link Dag} to the {@link DagStateStore}, and then submits it to the specific
 * {@link DagManagerThread}'s {@link BlockingQueue} based on the flowExecutionId of the Flow.
 * This guarantees that each {@link Dag} received by the {@link DagManager} can be recovered in case of a leadership
 * change or service restart.
 *
 * The implementation of the {@link DagManager} is multi-threaded. Each {@link DagManagerThread} polls the
 * {@link BlockingQueue} for new Dag submissions at fixed intervals. It deques any newly submitted Dags and coordinates
 * the execution of individual jobs in the Dag. The coordination logic involves polling the {@link JobStatus}es of running
 * jobs. Upon completion of a job, it will either schedule the next job in the Dag (on SUCCESS) or mark the Dag as failed
 * (on FAILURE). Upon completion of a Dag execution, it will perform the required clean up actions.
 *
 * For deleteSpec/cancellation requests for a flow URI, {@link DagManager} finds out the flowExecutionId using
 * {@link JobStatusRetriever}, and forwards the request to the {@link DagManagerThread} which handled the addSpec request
 * for this flow. We need separate {@link BlockingQueue}s for each {@link DagManagerThread} because
 * cancellation needs the information which is stored only in the same {@link DagManagerThread}.
 *
 * The {@link DagManager} is active only in the leader mode. To ensure, each {@link Dag} managed by a {@link DagManager} is
 * checkpointed to a persistent location. On start up or leadership change,
 * the {@link DagManager} loads all the checkpointed {@link Dag}s and adds them to the {@link  BlockingQueue}.
 * Current implementation supports only FileSystem-based checkpointing of the Dag statuses.
 */
@Alpha
@Slf4j
public class DagManager extends AbstractIdleService {
  public static final String DEFAULT_FLOW_FAILURE_OPTION = FailureOption.FINISH_ALL_POSSIBLE.name();

  public static final String DAG_MANAGER_PREFIX = "gobblin.service.dagManager.";

  private static final String JOB_STATUS_RETRIEVER_KEY = DAG_MANAGER_PREFIX + "jobStatusRetriever";
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;
  public static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer TERMINATION_TIMEOUT = 30;
  public static final String NUM_THREADS_KEY = DAG_MANAGER_PREFIX + "numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = DAG_MANAGER_PREFIX + "pollingInterval";
  private static final String JOB_STATUS_RETRIEVER_CLASS_KEY = JOB_STATUS_RETRIEVER_KEY + ".class";
  private static final String DEFAULT_JOB_STATUS_RETRIEVER_CLASS = FsJobStatusRetriever.class.getName();
  private static final String DAG_STATESTORE_CLASS_KEY = DAG_MANAGER_PREFIX + "dagStateStoreClass";
  private static final String USER_JOB_QUOTA_KEY = DAG_MANAGER_PREFIX + "defaultJobQuota";
  private static final Integer DEFAULT_USER_JOB_QUOTA = Integer.MAX_VALUE;
  private static final String PER_USER_QUOTA = DAG_MANAGER_PREFIX + "perUserQuota";

  private static final String QUOTA_SEPERATOR = ":";

  /**
   * Action to be performed on a {@link Dag}, in case of a job failure. Currently, we allow 2 modes:
   * <ul>
   *   <li> FINISH_RUNNING, which allows currently running jobs to finish.</li>
   *   <li> FINISH_ALL_POSSIBLE, which allows every possible job in the Dag to finish, as long as all the dependencies
   *   of the job are successful.</li>
   * </ul>
   */
  public enum FailureOption {
    FINISH_RUNNING("FINISH_RUNNING"),
    CANCEL("CANCEL"),
    FINISH_ALL_POSSIBLE("FINISH_ALL_POSSIBLE");

    private final String failureOption;

    FailureOption(final String failureOption) {
      this.failureOption = failureOption;
    }

    @Override
    public String toString() {
      return this.failureOption;
    }
  }

  private BlockingQueue<Dag<JobExecutionPlan>>[] queue;
  private BlockingQueue<String>[] cancelQueue;
  DagManagerThread[] dagManagerThreads;

  private ScheduledExecutorService scheduledExecutorPool;
  private boolean instrumentationEnabled;
  private DagStateStore dagStateStore;
  private Map<URI, TopologySpec> topologySpecMap;

  @Getter
  private final Integer numThreads;
  private final Integer pollingInterval;
  @Getter
  private final JobStatusRetriever jobStatusRetriever;
  private final Config config;
  private final Optional<EventSubmitter> eventSubmitter;
  private final int defaultQuota;
  private final Map<String, Integer> perUserQuota;

  private volatile boolean isActive = false;

  public DagManager(Config config, boolean instrumentationEnabled) {
    this.config = config;
    this.numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.queue = initializeDagQueue(this.numThreads);
    this.cancelQueue = initializeDagQueue(this.numThreads);
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);
    this.instrumentationEnabled = instrumentationEnabled;
    if (instrumentationEnabled) {
      MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    } else {
      this.eventSubmitter = Optional.absent();
    }

    this.defaultQuota = ConfigUtils.getInt(config, USER_JOB_QUOTA_KEY, DEFAULT_USER_JOB_QUOTA);
    this.perUserQuota = new HashMap<>();
    for (String userQuota : ConfigUtils.getStringList(config, PER_USER_QUOTA)) {
      this.perUserQuota.put(userQuota.split(QUOTA_SEPERATOR)[0], Integer.parseInt(userQuota.split(QUOTA_SEPERATOR)[1]));
    }

    try {
      this.jobStatusRetriever = createJobStatusRetriever(config);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Exception encountered during DagManager initialization", e);
    }
  }

  JobStatusRetriever createJobStatusRetriever(Config config) throws ReflectiveOperationException {
    Class jobStatusRetrieverClass = Class.forName(ConfigUtils.getString(config, JOB_STATUS_RETRIEVER_CLASS_KEY, DEFAULT_JOB_STATUS_RETRIEVER_CLASS));
    return (JobStatusRetriever) GobblinConstructorUtils.invokeLongestConstructor(jobStatusRetrieverClass, config);
  }

  KafkaJobStatusMonitor createJobStatusMonitor(Config config) throws ReflectiveOperationException {
    return new KafkaJobStatusMonitorFactory().createJobStatusMonitor(config);
  }

  DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    try {
      Class dagStateStoreClass = Class.forName(ConfigUtils.getString(config, DAG_STATESTORE_CLASS_KEY, FSDagStateStore.class.getName()));
      return (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config, topologySpecMap);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  // Initializes and returns an array of Queue of size numThreads
  private static LinkedBlockingDeque[] initializeDagQueue(int numThreads) {
    LinkedBlockingDeque[] queue = new LinkedBlockingDeque[numThreads];

    for (int i=0; i< numThreads; i++) {
      queue[i] = new LinkedBlockingDeque<>();
    }
    return queue;
  }

  public DagManager(Config config) {
    this(config, true);
  }

  /** Start the service. On startup, the service launches a fixed pool of {@link DagManagerThread}s, which are scheduled at
   * fixed intervals. The service also loads any {@link Dag}s
   */
  @Override
  protected void startUp() {
    //Do nothing.
  }

  /**
   * Method to submit a {@link Dag} to the {@link DagManager}. The {@link DagManager} optionally persists the
   * submitted dag to the {@link DagStateStore} and then adds the dag to a {@link BlockingQueue} to be picked up
   * by one of the {@link DagManagerThread}s.
   * @param dag {@link Dag} to be added
   * @param persist whether to persist the dag to the {@link DagStateStore}
   */
  synchronized void addDag(Dag<JobExecutionPlan> dag, boolean persist) throws IOException {
    if (persist) {
      //Persist the dag
      this.dagStateStore.writeCheckpoint(dag);
    }
    int queueId = DagManagerUtils.getDagQueueId(dag, this.numThreads);
    // Add the dag to the specific queue determined by flowExecutionId
    // Flow cancellation request has to be forwarded to the same DagManagerThread where the
    // flow create request was forwarded. This is because Azkaban Exec Id is stored in the DagNode of the
    // specific DagManagerThread queue
    if (!this.queue[queueId].offer(dag)) {
      throw new IOException("Could not add dag" + DagManagerUtils.generateDagId(dag) + "to queue");
    }
    submitEventsAndSetStatus(dag);
  }

  private void submitEventsAndSetStatus(Dag<JobExecutionPlan> dag) {
    if (this.eventSubmitter.isPresent()) {
      for (DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
        this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING).stop(jobMetadata);
        jobExecutionPlan.setExecutionStatus(PENDING);
      }
    }
  }

  /**
   * Method to submit a {@link URI} for cancellation requsts to the {@link DagManager}.
   * The {@link DagManager} adds the dag to the {@link BlockingQueue} to be picked up by one of the {@link DagManagerThread}s.
   */
  synchronized public void stopDag(URI uri) throws IOException {
    String flowGroup = FlowSpec.Utils.getFlowGroup(uri);
    String flowName = FlowSpec.Utils.getFlowName(uri);

    List<Long> flowExecutionIds = this.jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 10);
    log.info("Found {} flows to cancel.", flowExecutionIds.size());

    for (long flowExecutionId : flowExecutionIds) {
      killFlow(flowGroup, flowName, flowExecutionId);
    }
  }

  /**
   * Add the specified flow to {@link DagManager#cancelQueue}
   */
  private void killFlow(String flowGroup, String flowName, long flowExecutionId) throws IOException {
    int queueId =  DagManagerUtils.getDagQueueId(flowExecutionId, this.numThreads);
    String dagId = DagManagerUtils.generateDagId(flowGroup, flowName, flowExecutionId);
    if (!this.cancelQueue[queueId].offer(dagId)) {
      throw new IOException("Could not add dag " + dagId + " to cancellation queue.");
    }
  }

  @Subscribe
  public void handleKillFlowEvent(KillFlowEvent killFlowEvent) {
    log.info("Received kill request for flow ({}, {}, {})", killFlowEvent.getFlowGroup(), killFlowEvent.getFlowName(), killFlowEvent.getFlowExecutionId());
    try {
      killFlow(killFlowEvent.getFlowGroup(), killFlowEvent.getFlowName(), killFlowEvent.getFlowExecutionId());
    } catch (IOException e) {
      log.warn("Failed to kill flow", e);
    }
  }

  public synchronized void setTopologySpecMap(Map<URI, TopologySpec> topologySpecMap) {
    this.topologySpecMap = topologySpecMap;
  }

  /**
   * When a {@link DagManager} becomes active, it loads the serialized representations of the currently running {@link Dag}s
   * from the checkpoint directory, deserializes the {@link Dag}s and adds them to a queue to be consumed by
   * the {@link DagManagerThread}s.
   * @param active a boolean to indicate if the {@link DagManager} is the leader.
   */
  public synchronized void setActive(boolean active) {
    if (this.isActive == active) {
      log.info("DagManager already {}, skipping further actions.", (!active) ? "inactive" : "active");
      return;
    }
    this.isActive = active;
    try {
      if (this.isActive) {
        log.info("Activating DagManager.");
        log.info("Scheduling {} DagManager threads", numThreads);
        //Initializing state store for persisting Dags.
        this.dagStateStore = createDagStateStore(config, topologySpecMap);

        //On startup, the service creates DagManagerThreads that are scheduled at a fixed rate.
        this.dagManagerThreads = new DagManagerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
          DagManagerThread dagManagerThread = new DagManagerThread(jobStatusRetriever, dagStateStore,
              queue[i], cancelQueue[i], instrumentationEnabled, defaultQuota, perUserQuota);
          this.dagManagerThreads[i] = dagManagerThread;
          this.scheduledExecutorPool.scheduleAtFixedRate(dagManagerThread, 0, this.pollingInterval, TimeUnit.SECONDS);
        }
        List<Dag<JobExecutionPlan>> dags = dagStateStore.getDags();
        log.info("Loading " + dags.size() + " dags from dag state store");
        for (Dag<JobExecutionPlan> dag : dags) {
          addDag(dag, false);
        }
      } else { //Mark the DagManager inactive.
        log.info("Inactivating the DagManager. Shutting down all DagManager threads");
        this.scheduledExecutorPool.shutdown();
        try {
          this.scheduledExecutorPool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.error("Exception encountered when shutting down DagManager threads.", e);
        }
      }
    } catch (IOException e) {
      log.error("Exception encountered when activating the new DagManager", e);
      throw new RuntimeException(e);
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
    private final Map<DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new HashMap<>();
    private static final Map<String, Integer> proxyUserToJobCount = new ConcurrentHashMap<>();
    private static final Map<String, Integer> requesterToJobCount = new ConcurrentHashMap<>();
    private final Map<String, Dag<JobExecutionPlan>> dags = new HashMap<>();
    // dagToJobs holds a map of dagId to running jobs of that dag
    final Map<String, LinkedList<DagNode<JobExecutionPlan>>> dagToJobs = new HashMap<>();
    final Map<String, Long> dagToSLA = new HashMap<>();
    private final Set<String> failedDagIdsFinishRunning = new HashSet<>();
    private final Set<String> failedDagIdsFinishAllPossible = new HashSet<>();
    private final MetricContext metricContext;
    private final Optional<EventSubmitter> eventSubmitter;
    private final Optional<Timer> jobStatusPolledTimer;
    private final int defaultQuota;
    private final Map<String, Integer> perUserQuota;
    private final AtomicLong orchestrationDelay = new AtomicLong(0);

    private JobStatusRetriever jobStatusRetriever;
    private DagStateStore dagStateStore;
    private BlockingQueue<Dag<JobExecutionPlan>> queue;
    private BlockingQueue<String> cancelQueue;

    /**
     * Constructor.
     */
    DagManagerThread(JobStatusRetriever jobStatusRetriever, DagStateStore dagStateStore,
        BlockingQueue<Dag<JobExecutionPlan>> queue, BlockingQueue<String> cancelQueue, boolean instrumentationEnabled,
        int defaultQuota, Map<String, Integer> perUserQuota) {
      this.jobStatusRetriever = jobStatusRetriever;
      this.dagStateStore = dagStateStore;
      this.queue = queue;
      this.cancelQueue = cancelQueue;
      this.defaultQuota = defaultQuota;
      this.perUserQuota = perUserQuota;
      if (instrumentationEnabled) {
        this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
        this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
        this.jobStatusPolledTimer = Optional.of(this.metricContext.timer(ServiceMetricNames.JOB_STATUS_POLLED_TIMER));
        ContextAwareGauge<Long> orchestrationDelayMetric = metricContext.newContextAwareGauge(ServiceMetricNames.FLOW_ORCHESTRATION_DELAY,
            () -> orchestrationDelay.get());
        this.metricContext.register(orchestrationDelayMetric);
      } else {
        this.metricContext = null;
        this.eventSubmitter = Optional.absent();
        this.jobStatusPolledTimer = Optional.absent();
      }
    }

    /**
     * Main body of the {@link DagManagerThread}. Deque the next item from the queue and poll job statuses of currently
     * running jobs.
     * Because this thread runs in a regular interval, we should avoid doing repetitive work inside it.
     */
    @Override
    public void run() {
      try {
        String nextDagToCancel = cancelQueue.poll();
        //Poll the cancelQueue for a new Dag to cancel.
        if (nextDagToCancel != null) {
          cancelDag(nextDagToCancel);
        }

        while (!queue.isEmpty()) {
          Dag<JobExecutionPlan> dag = queue.poll();
          //Poll the queue for a new Dag to execute.
          if (dag != null) {
            if (dag.isEmpty()) {
              log.info("Empty dag; ignoring the dag");
            }
            //Initialize dag.
            initialize(dag);
          }
        }

        log.debug("Polling job statuses..");
        //Poll and update the job statuses of running jobs.
        pollAndAdvanceDag();
        log.debug("Poll done.");
        //Clean up any finished dags
        log.debug("Cleaning up finished dags..");
        cleanUp();
        log.debug("Clean up done");
      } catch (Exception e) {
        log.error("Exception encountered in {}", getClass().getName(), e);
      }
    }

    /**
     * Cancels the dag and sends a cancellation tracking event.
     * @param dagToCancel dag node to cancel
     * @throws ExecutionException executionException
     * @throws InterruptedException interruptedException
     */
    private void cancelDag(String dagToCancel) throws ExecutionException, InterruptedException {
      log.info("Cancel flow with DagId {}", dagToCancel);
      if (this.dagToJobs.containsKey(dagToCancel)) {
        List<DagNode<JobExecutionPlan>> dagNodesToCancel = this.dagToJobs.get(dagToCancel);
        log.info("Found {} DagNodes to cancel.", dagNodesToCancel.size());
        for (DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
          cancelDagNode(dagNodeToCancel);
        }

        this.dags.get(dagToCancel).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        this.dags.get(dagToCancel).setMessage("Flow killed by request");
      } else {
        log.warn("Did not find Dag with id {}, it might be already cancelled/finished.", dagToCancel);
      }
    }

    private void cancelDagNode(DagNode<JobExecutionPlan> dagNodeToCancel) throws ExecutionException, InterruptedException {
      Properties props = new Properties();
      if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
        Future future = dagNodeToCancel.getValue().getJobFuture().get();
        String serializedFuture = DagManagerUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
        props.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
        sendCancellationEvent(dagNodeToCancel.getValue());
      }
      DagManagerUtils.getSpecProducer(dagNodeToCancel).deleteSpec(dagNodeToCancel.getValue().getJobSpec().getUri(), props);
    }

    private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan) {
      if (this.eventSubmitter.isPresent()) {
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
        this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
        jobExecutionPlan.setExecutionStatus(CANCELLED);
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
      log.info("Initializing Dag {}", DagManagerUtils.getFullyQualifiedDagName(dag));
      if (this.dags.containsKey(dagId)) {
        log.warn("Already tracking a dag with dagId {}, skipping.", dagId);
        return;
      }

      this.dags.put(dagId, dag);
      log.debug("Dag {} - determining if any jobs are already running.", DagManagerUtils.getFullyQualifiedDagName(dag));

      //A flag to indicate if the flow is already running.
      boolean isDagRunning = false;
      //Are there any jobs already in the running state? This check is for Dags already running
      //before a leadership change occurs.
      for (DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        if (DagManagerUtils.getExecutionStatus(dagNode) == RUNNING) {
          addJobState(dagId, dagNode);
          //Update the running jobs counter.
          getRunningJobsCounter(dagNode).inc();
          isDagRunning = true;
        }
      }

      log.debug("Dag {} submitting jobs ready for execution.", DagManagerUtils.getFullyQualifiedDagName(dag));
      //Determine the next set of jobs to run and submit them for execution
      Map<String, Set<DagNode<JobExecutionPlan>>> nextSubmitted = submitNext(dagId);
      for (DagNode dagNode: nextSubmitted.get(dagId)) {
        addJobState(dagId, dagNode);
      }

      // Set flow status to running
      DagManagerUtils.emitFlowEvent(this.eventSubmitter, dag, TimingEvent.FlowTimings.FLOW_RUNNING);

      // Report the orchestration delay the first time the Dag is initialized. Orchestration delay is defined as
      // the time difference between the instant when a flow first transitions to the running state and the instant
      // when the flow is submitted to Gobblin service.
      if (!isDagRunning) {
        this.orchestrationDelay.set(System.currentTimeMillis() - DagManagerUtils.getFlowExecId(dag));
      }

      log.info("Dag {} Initialization complete.", DagManagerUtils.getFullyQualifiedDagName(dag));
    }

    /**
     * Proceed the execution of each dag node based on job status.
     */
    private void pollAndAdvanceDag() throws IOException, ExecutionException, InterruptedException {
      this.failedDagIdsFinishRunning.clear();
      Map<String, Set<DagNode<JobExecutionPlan>>> nextSubmitted = Maps.newHashMap();
      List<DagNode<JobExecutionPlan>> nodesToCleanUp = Lists.newArrayList();

      for (DagNode<JobExecutionPlan> node : this.jobToDag.keySet()) {
        boolean slaKilled = slaKillIfNeeded(node);

        JobStatus jobStatus = pollJobStatus(node);

        boolean killOrphanFlow = killJobIfOrphaned(node, jobStatus);

        ExecutionStatus status = getJobExecutionStatus(slaKilled, killOrphanFlow, jobStatus);

        JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(node);

        switch (status) {
          case COMPLETE:
            jobExecutionPlan.setExecutionStatus(COMPLETE);
            nextSubmitted.putAll(onJobFinish(node));
            nodesToCleanUp.add(node);
            break;
          case FAILED:
            jobExecutionPlan.setExecutionStatus(FAILED);
            nextSubmitted.putAll(onJobFinish(node));
            nodesToCleanUp.add(node);
            break;
          case CANCELLED:
            jobExecutionPlan.setExecutionStatus(CANCELLED);
            nextSubmitted.putAll(onJobFinish(node));
            nodesToCleanUp.add(node);
            break;
          case PENDING:
            jobExecutionPlan.setExecutionStatus(PENDING);
            break;
          case PENDING_RETRY:
            jobExecutionPlan.setExecutionStatus(PENDING_RETRY);
            break;
          default:
            jobExecutionPlan.setExecutionStatus(RUNNING);
            break;
        }

        if (jobStatus != null && jobStatus.isShouldRetry()) {
          log.info("Retrying job: {}, current attempts: {}, max attempts: {}", DagManagerUtils.getFullyQualifiedJobName(node),
              jobStatus.getCurrentAttempts(), jobStatus.getMaxAttempts());
          submitJob(node);
        }
      }

      for (Map.Entry<String, Set<DagNode<JobExecutionPlan>>> entry: nextSubmitted.entrySet()) {
        String dagId = entry.getKey();
        Set<DagNode<JobExecutionPlan>> dagNodes = entry.getValue();
        for (DagNode<JobExecutionPlan> dagNode: dagNodes) {
          addJobState(dagId, dagNode);
        }
      }

      for (DagNode<JobExecutionPlan> dagNode: nodesToCleanUp) {
        String dagId = DagManagerUtils.generateDagId(dagNode);
        deleteJobState(dagId, dagNode);
      }
    }

    /**
     * Cancel the job if the job has been "orphaned". A job is orphaned if has been in ORCHESTRATED
     * {@link ExecutionStatus} for some specific amount of time.
     * @param node {@link DagNode} representing the job
     * @param jobStatus current {@link JobStatus} of the job
     * @return true if the total time that the job remains in the ORCHESTRATED state exceeds
     * {@value ConfigurationKeys#GOBBLIN_JOB_START_SLA_TIME}.
     */
    private boolean killJobIfOrphaned(DagNode<JobExecutionPlan> node, JobStatus jobStatus)
        throws ExecutionException, InterruptedException {
      if (jobStatus == null) {
        return false;
      }
      ExecutionStatus executionStatus = valueOf(jobStatus.getEventName());
      long timeOutForJobStart = DagManagerUtils.getJobStartSla(node);
      long jobOrchestratedTime = jobStatus.getOrchestratedTime();

      if (executionStatus == ORCHESTRATED && System.currentTimeMillis() - jobOrchestratedTime > timeOutForJobStart) {
        log.info("Job {} of flow {} exceeded the job start SLA of {} ms. Killing the job now...",
            DagManagerUtils.getJobName(node),
            DagManagerUtils.getFullyQualifiedDagName(node),
            timeOutForJobStart);
        cancelDagNode(node);

        String dagId = DagManagerUtils.generateDagId(node);
        this.dags.get(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        this.dags.get(dagId).setMessage("Flow killed because no update received for " + timeOutForJobStart + " ms after orchestration");

        return true;
      } else {
        return false;
      }
    }

    private ExecutionStatus getJobExecutionStatus(boolean slaKilled, boolean killOrphanFlow, JobStatus jobStatus) {
      if (slaKilled || killOrphanFlow) {
        return CANCELLED;
      } else {
        if (jobStatus == null) {
          return PENDING;
        } else {
          return valueOf(jobStatus.getEventName());
        }
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
    private boolean slaKillIfNeeded(DagNode<JobExecutionPlan> node) throws ExecutionException, InterruptedException {
      long flowStartTime = DagManagerUtils.getFlowStartTime(node);
      long currentTime = System.currentTimeMillis();
      String dagId = DagManagerUtils.generateDagId(node);

      long flowSla;
      if (dagToSLA.containsKey(dagId)) {
        flowSla = dagToSLA.get(dagId);
      } else {
        flowSla = DagManagerUtils.getFlowSLA(node);
        dagToSLA.put(dagId, flowSla);
      }

      if (currentTime > flowStartTime + flowSla) {
        log.info("Flow {} exceeded the SLA of {} ms. Killing the job {} now...",
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), flowSla,
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY));
        cancelDagNode(node);

        this.dags.get(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        this.dags.get(dagId).setMessage("Flow killed due to exceeding SLA of " + flowSla + " ms");

        return true;
      }
      return false;
    }

    /**
     * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
     */
    private JobStatus pollJobStatus(DagNode<JobExecutionPlan> dagNode) {
      Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
      String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
      long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

      long pollStartTime = System.nanoTime();
      Iterator<JobStatus> jobStatusIterator =
          this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, jobName, jobGroup);
      Instrumented.updateTimer(this.jobStatusPolledTimer, System.nanoTime() - pollStartTime, TimeUnit.NANOSECONDS);

      if (jobStatusIterator.hasNext()) {
        return jobStatusIterator.next();
      } else {
        return null;
      }
    }

    /**
     * Submit next set of Dag nodes in the Dag identified by the provided dagId
     * @param dagId The dagId that should be processed.
     * @return
     * @throws IOException
     */
    synchronized Map<String, Set<DagNode<JobExecutionPlan>>> submitNext(String dagId) throws IOException {
      Dag<JobExecutionPlan> dag = this.dags.get(dagId);
      Set<DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

      //Submit jobs from the dag ready for execution.
      for (DagNode<JobExecutionPlan> dagNode : nextNodes) {
        submitJob(dagNode);
      }
      //Checkpoint the dag state
      this.dagStateStore.writeCheckpoint(dag);

      Map<String, Set<DagNode<JobExecutionPlan>>> dagIdToNextJobs = Maps.newHashMap();
      dagIdToNextJobs.put(dagId, nextNodes);
      return dagIdToNextJobs;
    }

    /**
     * Submits a {@link JobSpec} to a {@link org.apache.gobblin.runtime.api.SpecExecutor}.
     */
    private void submitJob(DagNode<JobExecutionPlan> dagNode) {
      DagManagerUtils.incrementJobAttempt(dagNode);
      JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
      jobExecutionPlan.setExecutionStatus(RUNNING);
      JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
      Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

      String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

      // Run this spec on selected executor
      SpecProducer producer = null;
      try {
        checkQuota(dagNode);
        producer = DagManagerUtils.getSpecProducer(dagNode);
        TimingEvent jobOrchestrationTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
            getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED) : null;

        // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
        // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
        // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
        // blocks (by calling Future#get()) until the submission is completed.
        Future addSpecFuture = producer.addSpec(jobSpec);
        dagNode.getValue().setJobFuture(Optional.of(addSpecFuture));
        //Persist the dag
        this.dagStateStore.writeCheckpoint(this.dags.get(DagManagerUtils.generateDagId(dagNode)));

        if (this.metricContext != null) {
          getRunningJobsCounter(dagNode).inc();
          getRunningJobsCounterForUser(dagNode).forEach(counter -> counter.inc());
        }

        addSpecFuture.get();

        jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));

        if (jobOrchestrationTimer != null) {
          jobOrchestrationTimer.stop(jobMetadata);
        }
        log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      } catch (Exception e) {
        TimingEvent jobFailedTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
            getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED) : null;
        String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
        log.error(message, e);
        jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
        if (jobFailedTimer != null) {
          jobFailedTimer.stop(jobMetadata);
        }
      }
    }

    private void checkQuota(DagNode<JobExecutionPlan> dagNode) throws IOException {
      String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
      String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);
      boolean proxyUserCheck = true;
      if (proxyUser != null) {
        proxyUserCheck = incrementMapAndCheckQuota(proxyUserToJobCount, proxyUser, dagNode);
      }

      String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
      boolean requesterCheck = true;
      String requesterMessage = null;
      if (serializedRequesters != null) {
        for (ServiceRequester requester : RequesterService.deserialize(serializedRequesters)) {
          requesterCheck &= incrementMapAndCheckQuota(requesterToJobCount, requester.getName(), dagNode);
          if (!requesterCheck && requesterMessage == null) {
            requesterMessage = "Quota exceeded for requester " + requester.getName() + " on executor " + specExecutorUri + ": quota="
                + getQuotaForUser(requester.getName()) + ", runningJobs=" + requesterToJobCount.get(DagManagerUtils.getUserQuotaKey(requester.getName(), dagNode));
          }
        }
      }

      // Throw errors for reach quota at the end to avoid inconsistent job counts
      if (!proxyUserCheck) {
        throw new IOException("Quota exceeded for proxy user " + proxyUser + " on executor " + specExecutorUri +
            ": quota=" + getQuotaForUser(proxyUser) + ", runningJobs=" + proxyUserToJobCount.get(DagManagerUtils.getUserQuotaKey(proxyUser, dagNode)));
      }

      if (!requesterCheck) {
        throw new IOException(requesterMessage);
      }
    }

    /**
     * Increment quota by one for the given map and key.
     * @return true if quota is not reached for this user or user is whitelisted, false otherwise.
     */
    private boolean incrementMapAndCheckQuota(Map<String, Integer> quotaMap, String user, DagNode<JobExecutionPlan> dagNode) {
      String key = DagManagerUtils.getUserQuotaKey(user, dagNode);
      int jobCount = quotaMap.getOrDefault(key, 0);

      // Only increment job count for first attempt, since job is considered running between retries
      if (dagNode.getValue().getCurrentAttempts() == 1) {
        jobCount++;
        quotaMap.put(key, jobCount);
      }

      return jobCount <= getQuotaForUser(user);
    }

    private int getQuotaForUser(String user) {
      return perUserQuota.getOrDefault(user, defaultQuota);
    }

    /**
     * Method that defines the actions to be performed when a job finishes either successfully or with failure.
     * This method updates the state of the dag and performs clean up actions as necessary.
     * TODO : Dag should have a status field, like JobExecutionPlan has. This method should update that field,
     *        which should be used by cleanup(). It may also remove the need of failedDagIdsFinishRunning,
     *        failedDagIdsFinishAllPossible.
     */
    private Map<String, Set<DagNode<JobExecutionPlan>>> onJobFinish(DagNode<JobExecutionPlan> dagNode)
        throws IOException {
      Dag<JobExecutionPlan> dag = this.jobToDag.get(dagNode);
      String dagId = DagManagerUtils.generateDagId(dag);
      String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
      ExecutionStatus jobStatus = DagManagerUtils.getExecutionStatus(dagNode);
      log.info("Job {} of Dag {} has finished with status {}", jobName, dagId, jobStatus.name());

      releaseQuota(dagNode);

      if (this.metricContext != null) {
        getRunningJobsCounter(dagNode).dec();
        getRunningJobsCounterForUser(dagNode).forEach(counter -> counter.dec());
      }

      switch (jobStatus) {
        // TODO : For now treat canceled as failed, till we introduce failure option - CANCEL
        case FAILED:
          dag.setMessage("Flow failed because job " + jobName + " failed");
          if (DagManagerUtils.getFailureOption(dag) == FailureOption.FINISH_RUNNING) {
            this.failedDagIdsFinishRunning.add(dagId);
          } else {
            this.failedDagIdsFinishAllPossible.add(dagId);
          }
          return Maps.newHashMap();
        case CANCELLED:
          if (DagManagerUtils.getFailureOption(dag) == FailureOption.FINISH_RUNNING) {
            this.failedDagIdsFinishRunning.add(dagId);
          } else {
            this.failedDagIdsFinishAllPossible.add(dagId);
          }
          return Maps.newHashMap();
        case COMPLETE:
          return submitNext(dagId);
        default:
          log.warn("It should not reach here. Job status is unexpected.");
          return Maps.newHashMap();
      }
    }

    /**
     * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link DagNode}.
     */
    private void releaseQuota(DagNode<JobExecutionPlan> dagNode) {
      String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
      if (proxyUser != null) {
        String proxyUserKey = DagManagerUtils.getUserQuotaKey(proxyUser, dagNode);
        if (proxyUserToJobCount.containsKey(proxyUserKey) && proxyUserToJobCount.get(proxyUserKey) > 0) {
          proxyUserToJobCount.put(proxyUserKey, proxyUserToJobCount.get(proxyUserKey) - 1);
        }
      }

      String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
      if (serializedRequesters != null) {
        try {
          for (ServiceRequester requester : RequesterService.deserialize(serializedRequesters)) {
            String requesterKey = DagManagerUtils.getUserQuotaKey(requester.getName(), dagNode);
            if (requesterToJobCount.containsKey(requesterKey) && requesterToJobCount.get(requesterKey) > 0) {
              requesterToJobCount.put(requesterKey, requesterToJobCount.get(requesterKey) - 1);
            }
          }
        } catch (IOException e) {
          log.error("Failed to release quota for requester list " + serializedRequesters, e);
        }
      }
    }

    private void deleteJobState(String dagId, DagNode<JobExecutionPlan> dagNode) {
      this.jobToDag.remove(dagNode);
      this.dagToJobs.get(dagId).remove(dagNode);
      this.dagToSLA.remove(dagId);
    }

    private void addJobState(String dagId, DagNode<JobExecutionPlan> dagNode) {
      Dag<JobExecutionPlan> dag = this.dags.get(dagId);
      this.jobToDag.put(dagNode, dag);
      if (this.dagToJobs.containsKey(dagId)) {
        this.dagToJobs.get(dagId).add(dagNode);
      } else {
        LinkedList<DagNode<JobExecutionPlan>> dagNodeList = Lists.newLinkedList();
        dagNodeList.add(dagNode);
        this.dagToJobs.put(dagId, dagNodeList);
      }
    }

    private boolean hasRunningJobs(String dagId) {
      return !this.dagToJobs.get(dagId).isEmpty();
    }

    private ContextAwareCounter getRunningJobsCounter(DagNode<JobExecutionPlan> dagNode) {
      return metricContext.contextAwareCounter(
          MetricRegistry.name(
              ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
              ServiceMetricNames.RUNNING_FLOWS_COUNTER,
              dagNode.getValue().getSpecExecutor().getUri().toString()));
    }

    private List<ContextAwareCounter> getRunningJobsCounterForUser(DagNode<JobExecutionPlan> dagNode) {
      Config configs = dagNode.getValue().getJobSpec().getConfig();
      String proxy = ConfigUtils.getString(configs, AzkabanProjectConfig.USER_TO_PROXY, null);
      List<ContextAwareCounter> counters = new ArrayList<>();

      if (StringUtils.isNotEmpty(proxy)) {
        counters.add(metricContext.contextAwareCounter(
            MetricRegistry.name(
                ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
                ServiceMetricNames.SERVICE_USERS, proxy)));
      }

      try {
        String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
        if (StringUtils.isNotEmpty(serializedRequesters)) {
          List<ServiceRequester> requesters = RequesterService.deserialize(serializedRequesters);
          for (ServiceRequester requester : requesters) {
            counters.add(metricContext.contextAwareCounter(MetricRegistry
                .name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.SERVICE_USERS, requester.getName())));
          }
        }
      } catch (IOException e) {
        log.error("Error while fetching requester list.", e);
      }

      return counters;
    }
    /**
     * Perform clean up. Remove a dag from the dagstore if the dag is complete and update internal state.
     */
    private void cleanUp() {
      List<String> dagIdstoClean = new ArrayList<>();
      //Clean up failed dags
      for (String dagId : this.failedDagIdsFinishRunning) {
        //Skip monitoring of any other jobs of the failed dag.
        LinkedList<DagNode<JobExecutionPlan>> dagNodeList = this.dagToJobs.get(dagId);
        while (!dagNodeList.isEmpty()) {
          DagNode<JobExecutionPlan> dagNode = dagNodeList.poll();
          deleteJobState(dagId, dagNode);
        }
        log.info("Dag {} has finished with status FAILED; Cleaning up dag from the state store.", dagId);
        // send an event before cleaning up dag
        DagManagerUtils.emitFlowEvent(this.eventSubmitter, this.dags.get(dagId), TimingEvent.FlowTimings.FLOW_FAILED);
        dagIdstoClean.add(dagId);
      }

      //Clean up completed dags
      for (String dagId : this.dags.keySet()) {
        if (!hasRunningJobs(dagId) && !this.failedDagIdsFinishRunning.contains(dagId)) {
          String status = TimingEvent.FlowTimings.FLOW_SUCCEEDED;
          if (this.failedDagIdsFinishAllPossible.contains(dagId)) {
            status = TimingEvent.FlowTimings.FLOW_FAILED;
            this.failedDagIdsFinishAllPossible.remove(dagId);
          }
          log.info("Dag {} has finished with status {}; Cleaning up dag from the state store.", dagId, status);
          // send an event before cleaning up dag
          DagManagerUtils.emitFlowEvent(this.eventSubmitter, this.dags.get(dagId), status);
          dagIdstoClean.add(dagId);
        }
      }

      for (String dagId: dagIdstoClean) {
        cleanUpDag(dagId);
      }
    }

    /**
     * Note that removal of a {@link Dag} entry in {@link #dags} needs to be happen after {@link #cleanUp()}
     * since the real {@link Dag} object is required for {@link #cleanUp()},
     * and cleaning of all relevant states need to be atomic
     * @param dagId
     */
    private synchronized void cleanUpDag(String dagId) {
       try {
         this.dagStateStore.cleanUp(dags.get(dagId));
       } catch (IOException ioe) {
         log.error(String.format("Failed to clean %s from backStore due to:", dagId), ioe);
       }
      this.dags.remove(dagId);
      this.dagToJobs.remove(dagId);
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
