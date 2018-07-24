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

package org.apache.gobblin.service.modules.flow;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Alpha
@Slf4j
public class FlowGraphPathFinder {
  private static final String SOURCE_PREFIX = "source";
  private static final String DESTINATION_PREFIX = "destination";

  private FlowGraph flowGraph;
  private FlowSpec flowSpec;
  private Config flowConfig;

  private DataNode srcNode;
  private DataNode destNode;

  private DatasetDescriptor srcDatasetDescriptor;
  private DatasetDescriptor destDatasetDescriptor;

  //Maintain path of FlowEdges as parent-child map
  private Map<FlowEdgeContext, FlowEdgeContext> pathMap;

  //Maintain a map from FlowEdge to SpecExecutor
  private Map<FlowEdge, SpecExecutor> flowEdgeToSpecExecutorMap;

  private Map<FlowEdge, Config> flowEdgeToMergedConfig;

  //Flow Execution Id
  private Long flowExecutionId;

  /**
   * Constructor.
   * @param flowGraph
   */
  public FlowGraphPathFinder(FlowGraph flowGraph, FlowSpec flowSpec) {
    this.flowGraph = flowGraph;
    this.flowSpec = flowSpec;
    this.flowConfig = flowSpec.getConfig();

    //Get src/dest DataNodes from the flow config
    String srcNodeId = ConfigUtils.getString(flowConfig, ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, "");
    String destNodeId = ConfigUtils.getString(flowConfig, ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "");
    this.srcNode = this.flowGraph.getNode(srcNodeId);
    Preconditions.checkArgument(srcNode != null, "Flowgraph does not have a node with id " + srcNodeId);
    this.destNode = this.flowGraph.getNode(destNodeId);
    Preconditions.checkArgument(destNode != null, "Flowgraph does not have a node with id " + destNodeId);

    //Get src/dest dataset descriptors from the flow config
    Config srcDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX);
    Config destDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX);

    try {
      Class srcdatasetDescriptorClass =
          Class.forName(srcDatasetDescriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
      this.srcDatasetDescriptor = (DatasetDescriptor) GobblinConstructorUtils
          .invokeLongestConstructor(srcdatasetDescriptorClass, srcDatasetDescriptorConfig);
      Class destDatasetDescriptorClass =
          Class.forName(destDatasetDescriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
      this.destDatasetDescriptor = (DatasetDescriptor) GobblinConstructorUtils
          .invokeLongestConstructor(destDatasetDescriptorClass, destDatasetDescriptorConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A helper class used to maintain additional context associated with each {@link FlowEdge} during path
   * computation while the edge is explored for its eligibility. The additional context includes the input
   * {@link DatasetDescriptor} of this edge which is compatible with the previous {@link FlowEdge}'s output
   * {@link DatasetDescriptor} (where "previous" means the immediately preceding {@link FlowEdge} visited before
   * the current {@link FlowEdge}), and the corresponding output dataset descriptor of the current {@link FlowEdge}.
   */
  @Data
  @AllArgsConstructor
  @Getter
  public static class FlowEdgeContext {
    private FlowEdge edge;
    private DatasetDescriptor inputDatasetDescriptor;
    private DatasetDescriptor outputDatasetDescriptor;
  }

  /**
   * A simple path finding algorithm based on Breadth-First Search. At every step the algorithm adds the adjacent {@link FlowEdge}s
   * to a queue. The {@link FlowEdge}s whose output {@link DatasetDescriptor} matches the destDatasetDescriptor are
   * added first to the queue. This ensures that dataset transformations are always performed closest to the source.
   * @return a list of {@link FlowEdge}s starting at the srcNode and ending at the destNode.
   */
  public Dag<JobExecutionPlan> findPath() throws PathFinderException {
    try {
      //Initialization of auxiliary data structures used for path computation
      this.pathMap = new HashMap<>();
      this.flowEdgeToSpecExecutorMap = new HashMap<>();
      this.flowEdgeToMergedConfig = new HashMap<>();

      // Generate flow execution id for this compilation
      this.flowExecutionId = System.currentTimeMillis();

      //Path computation must be thread-safe to guarantee read consistency. In other words, we prevent concurrent read/write access to the
      // flow graph.
      // TODO: we can easily improve the performance by using a ReentrantReadWriteLock associated with the FlowGraph. This will
      // allow multiple concurrent readers to not be blocked on each other, as long as there are no writers.
      synchronized (this.flowGraph) {
        //Base condition 1: Source Node or Dest Node is inactive; return null
        if (!srcNode.isActive() || !destNode.isActive()) {
          log.warn("Either source node {} or destination node {} is inactive; skipping path computation.", this.srcNode.getId(),
              this.destNode.getId());
          return null;
        }

        //Base condition 2: Check if we are already at the target. If so, return an empty dag.
        if ((srcNode.equals(destNode)) && destDatasetDescriptor.contains(srcDatasetDescriptor)) {
          return new Dag<>(new ArrayList<>());
        }

        LinkedList<FlowEdgeContext> edgeQueue = new LinkedList<>();
        edgeQueue.addAll(getNextEdges(srcNode, srcDatasetDescriptor, destDatasetDescriptor, flowConfig));
        for (FlowEdgeContext flowEdgeContext : edgeQueue) {
          this.pathMap.put(flowEdgeContext, flowEdgeContext);
        }

        //At every step, pop an edge E from the edge queue. Mark the edge E as visited. Generate the list of adjacent edges
        // to the edge E. For each adjacent edge E', do the following:
        //    1. check if the FlowTemplate described by E' is resolvable using the flowConfig, and
        //    2. check if the output dataset descriptor of edge E is compatible with the input dataset descriptor of the
        //       edge E'. If yes, add the edge E' to the edge queue.
        // If the edge E' satisfies 1 and 2, add it to the edge queue for further consideration.
        while (!edgeQueue.isEmpty()) {
          FlowEdgeContext flowEdgeContext = edgeQueue.pop();

          DataNode currentNode = this.flowGraph.getNode(flowEdgeContext.getEdge().getDest());
          DatasetDescriptor currentOutputDatasetDescriptor = flowEdgeContext.getOutputDatasetDescriptor();

          //Are we done?
          if (isPathFound(currentNode, destNode, currentOutputDatasetDescriptor, destDatasetDescriptor)) {
            return buildDag(flowEdgeContext);
          }

          //Expand the currentNode to its adjacent edges and add them to the queue.
          List<FlowEdgeContext> nextEdges =
              getNextEdges(currentNode, currentOutputDatasetDescriptor, destDatasetDescriptor, flowConfig);
          for (FlowEdgeContext childFlowEdgeContext : nextEdges) {
            //Add a pointer from the child edge to the parent edge, if the child edge is not already in the
            // queue.
            if (!this.pathMap.containsKey(childFlowEdgeContext)) {
              edgeQueue.add(childFlowEdgeContext);
              this.pathMap.put(childFlowEdgeContext, flowEdgeContext);
            }
          }
        }
      }
      //No path found. Return an empty dag.
      return new Dag<>(new ArrayList<>());
    } catch (SpecNotFoundException | JobTemplate.TemplateException | IOException | URISyntaxException e) {
      throw new PathFinderException(
          "Exception encountered when computing path from src: " + this.srcNode.getId() + " to dest: " + this.destNode.getId(), e);
    }
  }

  private boolean isPathFound(DataNode currentNode, DataNode destNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor) {
    if ((currentNode.equals(destNode)) && (currentDatasetDescriptor.equals(destDatasetDescriptor))) {
      return true;
    }
    return false;
  }

  /**
   * A helper method that sorts the {@link FlowEdge}s incident on srcNode based on whether the FlowEdge has an
   * output {@link DatasetDescriptor} that is compatible with the targetDatasetDescriptor.
   * @param dataNode
   * @param currentDatasetDescriptor Output {@link DatasetDescriptor} of the current edge.
   * @param destDatasetDescriptor Target {@link DatasetDescriptor}.
   * @return prioritized list of {@link FlowEdge}s to be added to the edge queue for expansion.
   */
  private List<FlowEdgeContext> getNextEdges(DataNode dataNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor, Config userConfig) {
    List<FlowEdgeContext> prioritizedEdgeList = new LinkedList<>();
    for (FlowEdge flowEdge : this.flowGraph.getEdges(dataNode)) {
      try {
        DataNode edgeDestination = this.flowGraph.getNode(flowEdge.getDest());
        //Should we skip this edge from any further consideration?
        if (!edgeDestination.isActive() || !flowEdge.isActive()) {
          continue;
        }
        for (Pair<DatasetDescriptor, DatasetDescriptor> datasetDescriptorPair : flowEdge.getFlowTemplate()
            .getInputOutputDatasetDescriptors(userConfig)) {
          DatasetDescriptor inputDatasetDescriptor = datasetDescriptorPair.getLeft();
          DatasetDescriptor outputDatasetDescriptor = datasetDescriptorPair.getRight();
          if (inputDatasetDescriptor.contains(currentDatasetDescriptor)) {
            Config inputDatasetDescriptorConfig = inputDatasetDescriptor.getRawConfig()
                .atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX);
            Config outputDatasetDescriptorConfig = outputDatasetDescriptor.getRawConfig()
                .atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX);
            userConfig = userConfig.withFallback(inputDatasetDescriptorConfig).withFallback(outputDatasetDescriptorConfig);
            if (!isFlowTemplateResolvable(flowEdge, userConfig)) {
              continue;
            }
            FlowEdgeContext flowEdgeContext;
            if (outputDatasetDescriptor.contains(currentDatasetDescriptor)) {
              //If datasets described by the currentDatasetDescriptor is a subset of the datasets described
              // by the outputDatasetDescriptor (i.e. currentDatasetDescriptor is more "specific" than outputDatasetDescriptor, e.g.
              // as in the case of a "distcp" edge), we propagate the more "specific" dataset descriptor forward.
              flowEdgeContext = new FlowEdgeContext(flowEdge, currentDatasetDescriptor, currentDatasetDescriptor);
            } else {
              //outputDatasetDescriptor is more specific (e.g. if it is a dataset transformation edge)
              flowEdgeContext = new FlowEdgeContext(flowEdge, currentDatasetDescriptor, outputDatasetDescriptor);
            }
            if (destDatasetDescriptor.getFormatDescriptor().contains(outputDatasetDescriptor.getFormatDescriptor())) {
              //Add to the front of the edge list if platform-independent properties of the output descriptor is compatible
              // with those of destination dataset descriptor.
              // In other words, we prioritize edges that perform data transformations as close to the source as possible.
              prioritizedEdgeList.add(0, flowEdgeContext);
            } else {
              prioritizedEdgeList.add(flowEdgeContext);
            }
          }
        }
      } catch (JobTemplate.TemplateException | SpecNotFoundException | IOException | ReflectiveOperationException
          | InterruptedException | ExecutionException e) {
        //Skip the edge; and continue
        log.warn("Skipping edge {} with config {} due to exception: {}", flowEdge.getId(), userConfig.toString(), e);
      }
    }
    return prioritizedEdgeList;
  }

  private boolean isFlowTemplateResolvable(FlowEdge flowEdge, Config userConfig)
      throws SpecNotFoundException, JobTemplate.TemplateException, ExecutionException, InterruptedException {
    Config srcNodeConfig = this.flowGraph.getNode(flowEdge.getSrc()).getRawConfig().atPath(SOURCE_PREFIX);
    Config destNodeConfig = this.flowGraph.getNode(flowEdge.getDest()).getRawConfig().atPath(DESTINATION_PREFIX);
    for (SpecExecutor specExecutor : flowEdge.getExecutors()) {
      // Build the "merged" config for each FlowEdge, which is a combination of (in the precedence described below):
      // 1. the user provided flow config,
      // 2. edge specific properties/overrides,
      // 3. SpecExecutor config/overrides,
      // 4. Source Node config, and
      // 5. Destination Node config.
      // Each JobTemplate's config will eventually be resolved against this merged config.
      Config mergedConfig = userConfig.withFallback(specExecutor.getConfig().get()).withFallback(flowEdge.getConfig())
          .withFallback(srcNodeConfig).withFallback(destNodeConfig);
      if (flowEdge.getFlowTemplate().isResolvable(mergedConfig)) {
        //Add the spec executor to the spec executor map.
        //TODO: Add the lowest-cost specExecutor instead of the first one that resolves the FlowTemplate.
        this.flowEdgeToSpecExecutorMap.put(flowEdge, specExecutor);
        this.flowEdgeToMergedConfig.put(flowEdge, mergedConfig);
        return true;
      }
    }
    return false;
  }

  /**
   *
   * @param flowEdgeContext of the last {@link FlowEdge} in the path.
   * @return a {@link Dag} of {@link JobExecutionPlan}s for the input {@link FlowSpec}.
   * @throws IOException
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   * @throws URISyntaxException
   */
  private Dag<JobExecutionPlan> buildDag(FlowEdgeContext flowEdgeContext)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    //Backtrace from the last edge using the path map and push each edge into a LIFO data structure.
    Deque<FlowEdge> path = new ArrayDeque<>();
    path.push(flowEdgeContext.getEdge());
    FlowEdgeContext currentFlowEdgeContext = flowEdgeContext;
    while (true) {
      path.push(this.pathMap.get(currentFlowEdgeContext).getEdge());
      currentFlowEdgeContext = this.pathMap.get(currentFlowEdgeContext);
      //Are we at the first edge in the path?
      if (this.pathMap.get(currentFlowEdgeContext).equals(currentFlowEdgeContext)) {
        break;
      }
    }
    //Build a DAG for each edge popped out of the LIFO data structure and concatenate it with the DAG generated from
    // the previously popped edges.
    Dag<JobExecutionPlan> flowDag = new Dag<>(new ArrayList<>());
    while (!path.isEmpty()) {
      FlowEdge flowEdge = path.pop();
      Config resolvedConfig = this.flowEdgeToMergedConfig.get(flowEdge);
      SpecExecutor specExecutor = this.flowEdgeToSpecExecutorMap.get(flowEdge);
      Dag<JobExecutionPlan> flowEdgeDag = convertHopToDag(flowEdge, flowSpec, resolvedConfig, specExecutor, flowExecutionId);
      flowDag = flowDag.concatenate(flowEdgeDag);
    }
    return flowDag;
  }

  /**
   * Given an instance of {@link FlowEdge}, this method returns a {@link Dag < JobExecutionPlan >} that moves data
   * from the source of the {@link FlowEdge} to the destination of the {@link FlowEdge}.
   * @param flowEdge an instance of {@link FlowEdge}.
   * @return a {@link Dag} of {@link JobExecutionPlan}s associated with the {@link FlowEdge}.
   */
  private Dag<JobExecutionPlan> convertHopToDag(FlowEdge flowEdge, FlowSpec flowSpec, Config resolvedConfig, SpecExecutor specExecutor, Long flowExecutionId)
      throws SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    //Iterate over each JobTemplate in the FlowEdge definition and convert the JobTemplate to a JobSpec by
    //resolving the JobTemplate config against the merged config.
    for (JobTemplate jobTemplate : flowEdge.getFlowTemplate().getJobTemplates()) {
      //Add job template to the resolved job config
      Config jobConfig = jobTemplate.getResolvedConfig(resolvedConfig).resolve().withValue(
          ConfigurationKeys.JOB_TEMPLATE_PATH, ConfigValueFactory.fromAnyRef(jobTemplate.getUri().toString()));
      //Add flow execution id to job config
      jobConfig = jobConfig.withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId));
      jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec, jobConfig, specExecutor));
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }


  public static class PathFinderException extends Exception {
    public PathFinderException(String message, Throwable cause) {
      super(message, cause);
    }

    public PathFinderException(String message) {
      super(message);
    }
  }
}

