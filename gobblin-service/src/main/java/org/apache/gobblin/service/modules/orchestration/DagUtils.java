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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.ServiceRequester;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine.FailureOption;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


public class DagUtils {
  static String QUOTA_KEY_SEPERATOR = ",";

  public static FlowId getFlowId(Dag<JobExecutionPlan> dag) {
    return getFlowId(dag.getStartNodes().get(0));
  }

  static FlowId getFlowId(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    return new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
  }

  public static long getFlowExecId(JobSpec jobSpec) {
    return jobSpec.getConfig().getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
  }


  /**
   * Generate a dagId object from the given {@link Dag} instance.
   * @param dag instance of a {@link Dag}.
   * @return a DagId object associated corresponding to the {@link Dag} instance.
   */
  public static Dag.DagId generateDagId(Dag<JobExecutionPlan> dag) {
    return generateDagId(dag.getStartNodes().get(0).getValue().getJobSpec().getConfig());
  }

  private static Dag.DagId generateDagId(Config jobConfig) {
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return new Dag.DagId(flowGroup, flowName, flowExecutionId);
  }

  public static Dag.DagId generateDagId(DagNode<JobExecutionPlan> dagNode) {
    return generateDagId(dagNode.getValue().getJobSpec().getConfig());
  }

  public static Dag.DagId generateDagId(String flowGroup, String flowName, long flowExecutionId) {
    return new Dag.DagId(flowGroup, flowName, flowExecutionId);
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
    Properties configAsProperties = (Properties) jobSpec.getConfigAsProperties().clone();
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
        switch (Objects.requireNonNull(failureOption)) {
          case FINISH_RUNNING:
            return new HashSet<>();
          // todo - FINISH_ALL_POSSIBLE should probably `continue` not `break`
          case FINISH_ALL_POSSIBLE:
          default:
            break;
        }
      }
    }
    return nextNodesToExecute;
  }

  public static FailureOption getFailureOption(Dag<JobExecutionPlan> dag) {
    if (dag.isEmpty()) {
      return null;
    }
    DagNode<JobExecutionPlan> dagNode = dag.getStartNodes().get(0);
    String failureOption = ConfigUtils.getString(getJobConfig(dagNode),
        ConfigurationKeys.FLOW_FAILURE_OPTION, DagProcessingEngine.DEFAULT_FLOW_FAILURE_OPTION);
    return DagProcessingEngine.FailureOption.valueOf(failureOption);
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

  public static long getFlowStartTime(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getFlowStartTime();
  }

  /**
   * get the sla from the dag node config.
   * if time unit is not provided, it assumes time unit is minute.
   * @param dagNode dag node for which sla is to be retrieved
   * @return sla if it is provided, DEFAULT_FLOW_SLA_MILLIS otherwise
   */
  public static long getFlowSLA(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    TimeUnit slaTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        jobConfig, ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigurationKeys.DEFAULT_GOBBLIN_FLOW_SLA_TIME_UNIT));

    return jobConfig.hasPath(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME)
        ? slaTimeUnit.toMillis(jobConfig.getLong(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME))
        : ServiceConfigKeys.DEFAULT_FLOW_SLA_MILLIS;
  }

  /**
   * get the job start sla from the dag node config.
   * if time unit is not provided, it assumes time unit is minute.
   * @param dagNode dag node for which flow start sla is to be retrieved
   * @return job start sla in ms
   */
  public static long getJobStartSla(DagNode<JobExecutionPlan> dagNode, Long defaultJobStartSla) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    TimeUnit slaTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        jobConfig, ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT, ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_SLA_TIME_UNIT));


    return jobConfig.hasPath(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME)
        ? slaTimeUnit.toMillis(jobConfig.getLong(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME))
        : defaultJobStartSla;
  }

  public static Config getDagJobConfig(Dag<JobExecutionPlan> dag) {
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

  public static DagNodeId calcJobId(Config jobConfig) {
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName =jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = ConfigUtils.getLong(jobConfig, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, 0L);
    String jobGroup = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_GROUP_KEY, "");
    String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");

    return new DagNodeId(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
  }
}
