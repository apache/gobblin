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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceRequester;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagManager.FailureOption;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


public class DagManagerUtils {
  static long DEFAULT_FLOW_SLA_MILLIS = TimeUnit.HOURS.toMillis(24);
  static String QUOTA_KEY_SEPERATOR = ",";

  public static FlowId getFlowId(Dag<JobExecutionPlan> dag) {
    return getFlowId(dag.getStartNodes().get(0));
  }

  public static DagActionStore.DagAction createDagActionFromDag(Dag<JobExecutionPlan> dag, DagActionStore.DagActionType dagActionType) {
    return createDagActionFromDagNode(dag.getStartNodes().get(0), dagActionType);
  }

  // todo - verify if jobName will always be present or need default and update FlowId, DagId etc to contain jobName
  public static DagActionStore.DagAction createDagActionFromDagNode(DagNode<JobExecutionPlan> dagNode, DagActionStore.DagActionType dagActionType) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName =  jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    String flowExecutionId = jobConfig.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    return new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType);
  }

  static FlowId getFlowId(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    return new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
  }

  public static long getFlowExecId(Dag<JobExecutionPlan> dag) {
    return getFlowExecId(dag.getStartNodes().get(0));
  }

  static long getFlowExecId(DagNode<JobExecutionPlan> dagNode) {
    return getFlowExecId(dagNode.getValue().getJobSpec());
  }

  static long getFlowExecId(JobSpec jobSpec) {
    return jobSpec.getConfig().getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
  }

  static long getFlowExecId(String dagId) {
    return Long.parseLong(dagId.substring(dagId.lastIndexOf('_') + 1));
  }


  /**
   * Generate a dagId object from the given {@link Dag} instance.
   * @param dag instance of a {@link Dag}.
   * @return a DagId object associated corresponding to the {@link Dag} instance.
   */
  public static DagManager.DagId generateDagId(Dag<JobExecutionPlan> dag) {
    return generateDagId(dag.getStartNodes().get(0).getValue().getJobSpec().getConfig());
  }

  private static DagManager.DagId generateDagId(Config jobConfig) {
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    String flowExecutionId = jobConfig.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return new DagManager.DagId(flowGroup, flowName, flowExecutionId);
  }

  public static DagManager.DagId generateDagId(DagNode<JobExecutionPlan> dagNode) {
    return generateDagId(dagNode.getValue().getJobSpec().getConfig());
  }

  public static DagManager.DagId generateDagId(String flowGroup, String flowName, long flowExecutionId) {
    return generateDagId(flowGroup, flowName, String.valueOf(flowExecutionId));
  }

  public static DagManager.DagId generateDagId(String flowGroup, String flowName, String flowExecutionId) {
    return new DagManager.DagId(flowGroup, flowName, flowExecutionId);
  }

  /**
   * Returns a fully-qualified {@link Dag} name that includes: (flowGroup, flowName, flowExecutionId).
   * @param dag
   * @return fully qualified name of the underlying {@link Dag}.
   */
  public static String getFullyQualifiedDagName(Dag<JobExecutionPlan> dag) {
    FlowId flowid = getFlowId(dag);
    long flowExecutionId = getFlowExecId(dag);
    return "(flowGroup: " + flowid.getFlowGroup() + ", flowName: " + flowid.getFlowName() + ", flowExecutionId: " + flowExecutionId + ")";
  }

  /**
   * Returns a fully-qualified {@link Dag} name that includes: (flowGroup, flowName, flowExecutionId).
   * @param dagNode
   * @return fully qualified name of the underlying {@link Dag}.
   */
  static String getFullyQualifiedDagName(DagNode<JobExecutionPlan> dagNode) {
    FlowId flowid = getFlowId(dagNode);
    long flowExecutionId = getFlowExecId(dagNode);
    return "(flowGroup: " + flowid.getFlowGroup() + ", flowName: " + flowid.getFlowName() + ", flowExecutionId: " + flowExecutionId + ")";
  }

  public static String getJobName(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY);
  }

  /**
   * Returns a fully-qualified job name that includes: (flowGroup, flowName, flowExecutionId, jobName).
   * @param dagNode
   * @return a fully qualified name of the underlying job.
   */
  public static String getFullyQualifiedJobName(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();

    String flowGroup = ConfigUtils.getString(jobConfig, ConfigurationKeys.FLOW_GROUP_KEY, "");
    String flowName = ConfigUtils.getString(jobConfig, ConfigurationKeys.FLOW_NAME_KEY, "");
    Long flowExecutionId = ConfigUtils.getLong(jobConfig, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, 0L);
    String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");

    return "(flowGroup: " + flowGroup + ", flowName: " + flowName + ", flowExecutionId: " + flowExecutionId + ", jobName: " + jobName + ")";
  }

  public static JobExecutionPlan getJobExecutionPlan(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue();
  }

  public static JobSpec getJobSpec(DagNode<JobExecutionPlan> dagNode) {
    JobSpec jobSpec = dagNode.getValue().getJobSpec();
    Map<String, String> configWithCurrentAttempts = ImmutableMap.of(ConfigurationKeys.JOB_CURRENT_ATTEMPTS, String.valueOf(dagNode.getValue().getCurrentAttempts()),
        ConfigurationKeys.JOB_CURRENT_GENERATION, String.valueOf(dagNode.getValue().getCurrentGeneration()));
    Properties configAsProperties = new Properties(jobSpec.getConfigAsProperties());
    configAsProperties.putAll(configWithCurrentAttempts);
    //Return new spec with new config to avoid change the reference to dagNode
    return new JobSpec(jobSpec.getUri(), jobSpec.getVersion(), jobSpec.getDescription(), ConfigFactory.parseMap(configWithCurrentAttempts).withFallback(jobSpec.getConfig()),
        configAsProperties, jobSpec.getTemplateURI(), jobSpec.getJobTemplate(), jobSpec.getMetadata());
  }

  static Config getJobConfig(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig();
  }

  public static SpecProducer<Spec> getSpecProducer(DagNode<JobExecutionPlan> dagNode)
      throws ExecutionException, InterruptedException {
    return dagNode.getValue().getSpecExecutor().getProducer().get();
  }

  public static ExecutionStatus getExecutionStatus(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getExecutionStatus();
  }

  /**
   * Traverse the dag to determine the next set of nodes to be executed. It starts with the startNodes of the dag and
   * identifies each node yet to be executed and for which each of its parent nodes is in the {@link ExecutionStatus#COMPLETE}
   * state.
   */
  public static Set<DagNode<JobExecutionPlan>> getNext(Dag<JobExecutionPlan> dag) {
    Set<DagNode<JobExecutionPlan>> nextNodesToExecute = new HashSet<>();
    LinkedList<DagNode<JobExecutionPlan>> nodesToExpand = Lists.newLinkedList(dag.getStartNodes());
    FailureOption failureOption = getFailureOption(dag);

    while (!nodesToExpand.isEmpty()) {
      DagNode<JobExecutionPlan> node = nodesToExpand.poll();
      ExecutionStatus executionStatus = getExecutionStatus(node);
      boolean addFlag = true;
      if (executionStatus == PENDING || executionStatus == PENDING_RETRY || executionStatus == PENDING_RESUME) {
        //Add a node to be executed next, only if all of its parent nodes are COMPLETE.
        List<DagNode<JobExecutionPlan>> parentNodes = dag.getParents(node);
        for (DagNode<JobExecutionPlan> parentNode : parentNodes) {
          if (getExecutionStatus(parentNode) != COMPLETE) {
            addFlag = false;
            break;
          }
        }
        if (addFlag) {
          nextNodesToExecute.add(node);
        }
      } else if (executionStatus == COMPLETE) {
        //Explore the children of COMPLETED node as next candidates for execution.
        nodesToExpand.addAll(dag.getChildren(node));
      } else if ((executionStatus == FAILED) || (executionStatus == CANCELLED)) {
        switch (failureOption) {
          case FINISH_RUNNING:
            return new HashSet<>();
          case FINISH_ALL_POSSIBLE:
          default:
            break;
        }
      }
    }
    return nextNodesToExecute;
  }

  static FailureOption getFailureOption(Dag<JobExecutionPlan> dag) {
    if (dag.isEmpty()) {
      return null;
    }
    DagNode<JobExecutionPlan> dagNode = dag.getStartNodes().get(0);
    String failureOption = ConfigUtils.getString(getJobConfig(dagNode),
        ConfigurationKeys.FLOW_FAILURE_OPTION, DagManager.DEFAULT_FLOW_FAILURE_OPTION);
    return FailureOption.valueOf(failureOption);
  }

  public static String getSpecExecutorUri(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getSpecExecutor().getUri().toString();
  }

  static String getSerializedRequesterList(DagNode<JobExecutionPlan> dagNode) {
    return ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), RequesterService.REQUESTER_LIST, null);
  }

  static String getUserQuotaKey(String user, DagNode<JobExecutionPlan> dagNode) {
    return user + QUOTA_KEY_SEPERATOR + getSpecExecutorUri(dagNode);
  }

  static String getFlowGroupQuotaKey(String flowGroup, DagNode<JobExecutionPlan> dagNode) {
    return flowGroup + QUOTA_KEY_SEPERATOR + getSpecExecutorUri(dagNode);
  }
  /**
   * Increment the value of {@link JobExecutionPlan#currentAttempts}
   */
  public static void incrementJobAttempt(DagNode<JobExecutionPlan> dagNode) {
    dagNode.getValue().setCurrentAttempts(dagNode.getValue().getCurrentAttempts() + 1);
  }

  /**
   * Increment the value of {@link JobExecutionPlan#currentGeneration}
   * This method is not thread safe, we achieve correctness by making sure
   * one dag will only be handled in the same DagManagerThread
   */
  public static void incrementJobGeneration(DagNode<JobExecutionPlan> dagNode) {
    dagNode.getValue().setCurrentGeneration(dagNode.getValue().getCurrentGeneration() + 1);
  }

  /**
   * Flow start time is the same as the flow execution id which is the timestamp flow request was received, unless it
   * is a resumed flow, in which case it is {@link JobExecutionPlan#getFlowStartTime()}
   * @param dagNode dag node in context
   * @return flow start time
   */
  static long getFlowStartTime(DagNode<JobExecutionPlan> dagNode) {
    long flowStartTime = dagNode.getValue().getFlowStartTime();
    return flowStartTime == 0L ? getFlowExecId(dagNode) : flowStartTime;
  }

  /**
   * get the sla from the dag node config.
   * if time unit is not provided, it assumes time unit is minute.
   * @param dagNode dag node for which sla is to be retrieved
   * @return sla if it is provided, DEFAULT_FLOW_SLA_MILLIS otherwise
   */
  static long getFlowSLA(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    TimeUnit slaTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        jobConfig, ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigurationKeys.DEFAULT_GOBBLIN_FLOW_SLA_TIME_UNIT));

    return jobConfig.hasPath(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME)
        ? slaTimeUnit.toMillis(jobConfig.getLong(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME))
        : DEFAULT_FLOW_SLA_MILLIS;
  }

  /**
   * get the job start sla from the dag node config.
   * if time unit is not provided, it assumes time unit is minute.
   * @param dagNode dag node for which flow start sla is to be retrieved
   * @return job start sla in ms
   */
  static long getJobStartSla(DagNode<JobExecutionPlan> dagNode, Long defaultJobStartSla) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    TimeUnit slaTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        jobConfig, ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT, ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_SLA_TIME_UNIT));


    return jobConfig.hasPath(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME)
        ? slaTimeUnit.toMillis(jobConfig.getLong(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME))
        : defaultJobStartSla;
  }

  static int getDagQueueId(Dag<JobExecutionPlan> dag, int numThreads) {
    return getDagQueueId(DagManagerUtils.getFlowExecId(dag), numThreads);
  }

  static int getDagQueueId(long flowExecutionId, int numThreads) {
    return (int) (flowExecutionId % numThreads);
  }

  static Config getDagJobConfig(Dag<JobExecutionPlan> dag) {
    // Every dag should have at least one node, and the job configurations are cloned among each node
    return dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
  }

  static boolean shouldFlowOutputMetrics(Dag<JobExecutionPlan> dag) {
    // defaults to false (so metrics are still tracked) if the dag property is not configured due to old dags
    return ConfigUtils.getBoolean(getDagJobConfig(dag), ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, true);
  }

  static String getSpecExecutorName(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getSpecExecutor().getUri().toString();
  }

  public static void emitFlowEvent(EventSubmitter eventSubmitter, Dag<JobExecutionPlan> dag, String flowEvent) {
    if (!dag.isEmpty()) {
      // Every dag node will contain the same flow metadata
      Config config = getDagJobConfig(dag);
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(config);

      if (dag.getFlowEvent() != null) {
        flowEvent = dag.getFlowEvent();
      }
      if (dag.getMessage() != null) {
        flowMetadata.put(TimingEvent.METADATA_MESSAGE, dag.getMessage());
      }

      eventSubmitter.getTimingEvent(flowEvent).stop(flowMetadata);
    }
  }

  static List<String> getDistinctUniqueRequesters(String serializedRequesters) {
    if (serializedRequesters == null) {
      return Collections.emptyList();
    }

    List<String> uniqueRequesters;
    try {
      uniqueRequesters = RequesterService.deserialize(serializedRequesters)
          .stream()
          .map(ServiceRequester::getName)
          .distinct()
          .collect(Collectors.toList());
      return uniqueRequesters;
    } catch (IOException e) {
      throw new RuntimeException("Could not process requesters due to ", e);
    }
  }

  /**
   * Set the execution status of all the {@link DagNode}s of the provided dag.
   * Also emits a {@link TimingEvent.LauncherTimings#JOB_PENDING} for each of those dag nodes.
   * @param dag dag whose status is to change.
   * @param eventSubmitter event submitter that will send the event.
   */
  public static void submitPendingExecStatus(Dag<JobExecutionPlan> dag, EventSubmitter eventSubmitter) {
    for (DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
      JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
      Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
      eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING).stop(jobMetadata);
      jobExecutionPlan.setExecutionStatus(PENDING);
    }
  }

  /**
   * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
   */
  public static java.util.Optional<JobStatus> pollJobStatus(DagNode<JobExecutionPlan> dagNode, JobStatusRetriever jobStatusRetriever, Timer jobStatusPolledTimer) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

    return pollStatus(flowGroup, flowName, flowExecutionId, jobGroup, jobName, jobStatusRetriever, jobStatusPolledTimer);
  }

  /**
   * Retrieve the flow's {@link JobStatus} (i.e. job status with {@link JobStatusRetriever#NA_KEY} as job name/group) from a dag
   * Returns empty optional if dag is null/empty or job status is not found.
   */
  public static java.util.Optional<JobStatus> pollFlowStatus(Dag<JobExecutionPlan> dag, JobStatusRetriever jobStatusRetriever, Timer jobStatusPolledTimer) {
    if (dag == null || dag.isEmpty()) {
      return java.util.Optional.empty();
    }
    Config jobConfig = dag.getNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return pollStatus(flowGroup, flowName, flowExecutionId, JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY, jobStatusRetriever, jobStatusPolledTimer);
  }

  /**
   * Retrieve the flow's {@link JobStatus} and update the timer if jobStatusPolledTimer is present.
   * Returns empty optional if job status is not found.
   */
  public static java.util.Optional<JobStatus> pollStatus(String flowGroup, String flowName, long flowExecutionId, String jobGroup, String jobName,
                                               JobStatusRetriever jobStatusRetriever, Timer jobStatusPolledTimer) {
    long pollStartTime = System.nanoTime();
    Iterator<JobStatus> jobStatusIterator =
        jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, jobName, jobGroup);
    Instrumented.updateTimer(jobStatusPolledTimer, System.nanoTime() - pollStartTime, TimeUnit.NANOSECONDS);

    if (jobStatusIterator.hasNext()) {
      return java.util.Optional.of(jobStatusIterator.next());
    } else {
      return java.util.Optional.empty();
    }
  }

  public static boolean hasRunningJobs(String dagId, Map<String, LinkedList<DagNode<JobExecutionPlan>>> dagToJobs) {
    List<DagNode<JobExecutionPlan>> dagNodes = dagToJobs.get(dagId);
    return dagNodes != null && !dagNodes.isEmpty();
  }

  public static DagNodeId calcJobId(Config jobConfig) {
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName =jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = ConfigUtils.getLong(jobConfig, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, 0L);
    String jobGroup = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_GROUP_KEY, "");
    String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");

    return new DagNodeId(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
  }
}
