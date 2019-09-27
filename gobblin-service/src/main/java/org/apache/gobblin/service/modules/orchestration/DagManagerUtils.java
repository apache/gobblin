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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.orchestration.DagManager.FailureOption;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


public class DagManagerUtils {
  static long NO_SLA = -1L;

  static FlowId getFlowId(Dag<JobExecutionPlan> dag) {
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    return new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
  }

  static long getFlowExecId(Dag<JobExecutionPlan> dag) {
    return getFlowExecId(dag.getStartNodes().get(0));
  }

  static long getFlowExecId(DagNode<JobExecutionPlan> dagNode) {
    return getFlowExecId(dagNode.getValue().getJobSpec());
  }

  static long getFlowExecId(JobSpec jobSpec) {
    return jobSpec.getConfig().getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
  }

  /**
   * Generate a dagId from the given {@link Dag} instance.
   * @param dag instance of a {@link Dag}.
   * @return a String id associated corresponding to the {@link Dag} instance.
   */
  static String generateDagId(Dag<JobExecutionPlan> dag) {
    return generateDagId(dag.getStartNodes().get(0).getValue().getJobSpec().getConfig());
  }

  static String generateDagId(Dag.DagNode<JobExecutionPlan> dagNode) {
    return generateDagId(dagNode.getValue().getJobSpec().getConfig());
  }

  private static String generateDagId(Config jobConfig) {
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    return generateDagId(flowGroup, flowName, flowExecutionId);
  }

  static String generateDagId(String flowGroup, String flowName, long flowExecutionId) {
    return Joiner.on("_").join(flowGroup, flowName, flowExecutionId);
  }

  /**
   * Generate a FlowId from the given {@link Dag} instance.
   * FlowId, comparing to DagId, doesn't contain FlowExecutionId so different {@link Dag} could possibly have same
   * {@link FlowId}.
   * @param dag
   * @return
   */
  static String generateFlowIdInString(Dag<JobExecutionPlan> dag) {
    FlowId flowId = getFlowId(dag);
    return Joiner.on("_").join(flowId.getFlowGroup(), flowId.getFlowName());
  }

  /**
   * Returns a fully-qualified {@link Dag} name that includes: (flowGroup, flowName, flowExecutionId).
   * @param dag
   * @return fully qualified name of the underlying {@link Dag}.
   */
  static String getFullyQualifiedDagName(Dag<JobExecutionPlan> dag) {
    FlowId flowid = getFlowId(dag);
    long flowExecutionId = getFlowExecId(dag);
    return "(flowGroup: " + flowid.getFlowGroup() + ", flowName: " + flowid.getFlowName() + ", flowExecutionId: " + flowExecutionId + ")";
  }

  static String getJobName(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY);
  }

  /**
   * Returns a fully-qualified job name that includes: (flowGroup, flowName, flowExecutionId, jobName).
   * @param dagNode
   * @return a fully qualified name of the underlying job.
   */
  static String getFullyQualifiedJobName(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();

    String flowGroup = ConfigUtils.getString(jobConfig, ConfigurationKeys.FLOW_GROUP_KEY, "");
    String flowName = ConfigUtils.getString(jobConfig, ConfigurationKeys.FLOW_NAME_KEY, "");
    Long flowExecutionId = ConfigUtils.getLong(jobConfig, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, 0L);
    String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");

    return "(flowGroup: " + flowGroup + ", flowName: " + flowName + ", flowExecutionId: " + flowExecutionId + ", jobName: " + jobName + ")";
  }

  static JobExecutionPlan getJobExecutionPlan(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue();
  }

  public static JobSpec getJobSpec(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec();
  }

  static Config getJobConfig(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig();
  }

  static SpecProducer getSpecProducer(DagNode<JobExecutionPlan> dagNode)
      throws ExecutionException, InterruptedException {
    return dagNode.getValue().getSpecExecutor().getProducer().get();
  }

  static ExecutionStatus getExecutionStatus(DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getExecutionStatus();
  }

  /**
   * Traverse the dag to determine the next set of nodes to be executed. It starts with the startNodes of the dag and
   * identifies each node yet to be executed and for which each of its parent nodes is in the {@link ExecutionStatus#COMPLETE}
   * state.
   */
  static Set<DagNode<JobExecutionPlan>> getNext(Dag<JobExecutionPlan> dag) {
    Set<DagNode<JobExecutionPlan>> nextNodesToExecute = new HashSet<>();
    LinkedList<DagNode<JobExecutionPlan>> nodesToExpand = Lists.newLinkedList(dag.getStartNodes());
    FailureOption failureOption = getFailureOption(dag);

    while (!nodesToExpand.isEmpty()) {
      DagNode<JobExecutionPlan> node = nodesToExpand.poll();
      ExecutionStatus executionStatus = getExecutionStatus(node);
      boolean addFlag = true;
      if (executionStatus == ExecutionStatus.PENDING) {
        //Add a node to be executed next, only if all of its parent nodes are COMPLETE.
        List<DagNode<JobExecutionPlan>> parentNodes = dag.getParents(node);
        for (DagNode<JobExecutionPlan> parentNode : parentNodes) {
          if (getExecutionStatus(parentNode) != ExecutionStatus.COMPLETE) {
            addFlag = false;
            break;
          }
        }
        if (addFlag) {
          nextNodesToExecute.add(node);
        }
      } else if (executionStatus == ExecutionStatus.COMPLETE) {
        //Explore the children of COMPLETED node as next candidates for execution.
        nodesToExpand.addAll(dag.getChildren(node));
      } else if ((executionStatus == ExecutionStatus.FAILED) || (executionStatus == ExecutionStatus.CANCELLED)) {
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

  /**
   * Increment the value of {@link JobExecutionPlan#currentAttempts}
   */
  static void incrementJobAttempt(DagNode<JobExecutionPlan> dagNode) {
    dagNode.getValue().setCurrentAttempts(dagNode.getValue().getCurrentAttempts() + 1);
  }

  /**
   * flow start time is assumed to be same the flow execution id which is timestamp flow request was received
   * @param dagNode dag node in context
   * @return flow execution id
   */
  static long getFlowStartTime(DagNode<JobExecutionPlan> dagNode) {
    return getFlowExecId(dagNode);
  }

  /**
   * get the sla from the dag node config.
   * if time unit is not provided, it assumes time unit is minute.
   * @param dagNode dag node for which sla is to be retrieved
   * @return sla if it is provided, {@value NO_SLA} otherwise
   */
  static long getFlowSLA(DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    TimeUnit slaTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        jobConfig, ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigurationKeys.DEFAULT_GOBBLIN_FLOW_SLA_TIME_UNIT));

    return jobConfig.hasPath(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME)
        ? slaTimeUnit.toMillis(jobConfig.getLong(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME))
        : NO_SLA;
  }

  static int getDagQueueId(Dag<JobExecutionPlan> dag, int numThreads) {
    return getDagQueueId(DagManagerUtils.getFlowExecId(dag), numThreads);
  }

  static int getDagQueueId(long flowExecutionId, int numThreads) {
    return (int) (flowExecutionId % numThreads);
  }

  static void emitFlowEvent(Optional<EventSubmitter> eventSubmitter, Dag<JobExecutionPlan> dag, String flowEvent) {
    if (eventSubmitter.isPresent() && !dag.isEmpty()) {
      // Every dag node will contain the same flow metadata
      Config config = dag.getNodes().get(0).getValue().getJobSpec().getConfig();
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(config);
      eventSubmitter.get().getTimingEvent(flowEvent).stop(flowMetadata);
    }
  }
}
