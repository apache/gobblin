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
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

public class DagManagerUtils {
  /**
   * Generate a dagId from the given {@link Dag} instance.
   * @param dag instance of a {@link Dag}.
   * @return a String id associated corresponding to the {@link Dag} instance.
   */
  public static String generateDagId(Dag<JobExecutionPlan> dag) {
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    Long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    return Joiner.on("_").join(flowGroup, flowName, flowExecutionId);
  }

  public static String getJobName(Dag.DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY);
  }

  public static JobExecutionPlan getJobExecutionPlan(Dag.DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue();
  }

  public static JobSpec getJobSpec(Dag.DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec();
  }

  public static Config getJobConfig(Dag.DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getJobSpec().getConfig();
  }

  public static SpecProducer getSpecProducer(Dag.DagNode<JobExecutionPlan> dagNode)
      throws ExecutionException, InterruptedException {
    return dagNode.getValue().getSpecExecutor().getProducer().get();
  }

  public static ExecutionStatus getExecutionStatus(Dag.DagNode<JobExecutionPlan> dagNode) {
    return dagNode.getValue().getExecutionStatus();
  }

  /**
   * Traverse the dag to determine the next set of nodes to be executed. It starts with the startNodes of the dag and
   * identifies each node yet to be executed and for which each of its parent nodes is in the {@link ExecutionStatus#COMPLETE}
   * state.
   */
  public static Set<Dag.DagNode<JobExecutionPlan>> getNext(Dag<JobExecutionPlan> dag) {
    Set<Dag.DagNode<JobExecutionPlan>> nextNodesToExecute = new HashSet<>();
    LinkedList<Dag.DagNode<JobExecutionPlan>> nodesToExpand = Lists.newLinkedList(dag.getStartNodes());

    while (!nodesToExpand.isEmpty()) {
      Dag.DagNode<JobExecutionPlan> node = nodesToExpand.poll();
      boolean addFlag = true;
      if (getExecutionStatus(node) == ExecutionStatus.$UNKNOWN) {
        //Add a node to be executed next, only if all of its parent nodes are COMPLETE.
        List<Dag.DagNode<JobExecutionPlan>> parentNodes = dag.getParents(node);
        for (Dag.DagNode<JobExecutionPlan> parentNode : parentNodes) {
          if (getExecutionStatus(parentNode) != ExecutionStatus.COMPLETE) {
            addFlag = false;
            break;
          }
        }
        if (addFlag) {
          nextNodesToExecute.add(node);
        }
      } else if (getExecutionStatus(node) == ExecutionStatus.COMPLETE) {
        //Explore the children of COMPLETED node as next candidates for execution.
        nodesToExpand.addAll(dag.getChildren(node));
      } else {
        return new HashSet<>();
      }
    }
    return nextNodesToExecute;
  }

}
