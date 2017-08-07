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

package gobblin.service.modules.flow;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import gobblin.runtime.api.BaseServiceNodeImpl;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecExecutor;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.job_spec.ResolvedJobSpec;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.WeightedMultigraph;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;

import gobblin.runtime.api.FlowEdge;
import gobblin.runtime.api.ServiceNode;
import gobblin.runtime.api.FlowSpec;
import gobblin.instrumented.Instrumented;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.TopologySpec;
import gobblin.service.ServiceConfigKeys;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

// Users are capable to inject hints/prioritization into route selection, in two forms:
// 1. PolicyBasedBlockedConnection: Define some undesired routes
// 2. Specified a complete path. FlowCompiler is responsible to verify if the path given is valid.

@Slf4j
public class MultiHopsFlowToJobSpecCompiler extends BaseFlowToJobSpecCompiler {

  /* TODO:
    1. When the connection between hops is no longer configuration based,
     should ensure the updates of following two data structures atomic with TopologySpec Catagory updates
    2. Figure out a way to inject weight information in Topology. We provide listenable interface in weightGraph, so
     those changes in topology should reflect in weightGraph as well.
  */

  @Getter
  private WeightedMultigraph<ServiceNode, FlowEdge> weightedGraph =
      new WeightedMultigraph<>(LoadBasedFlowEdgeImpl.class);

  //Contains the user-specified connection that are not desired to appear in JobSpec.
  //It can be used for avoiding known expensive or undesired data movement.
  public Optional<Multimap<String, String>> optionalPolicyBasedBlockedConnection;
  // Contains user-specified complete path of how the data movement is executed from source to sink.
  private Optional<String> optionalUserSpecifiedPath;
  // If the weight of edge in topology is considered in path finding process.
  // TODO: The way for injecting/updating edge weight need to be defined.
  private boolean edgeWeightEnabled = false;


  private FlowEdgeProps defaultFlowEdgeProps = new FlowEdgeProps(new Properties());


  public MultiHopsFlowToJobSpecCompiler(Config config){
    this(config, Optional.absent(), true);
  }

  public MultiHopsFlowToJobSpecCompiler(Config config, Optional<Logger> log){
    this(config, log, true);
  }

  public MultiHopsFlowToJobSpecCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled) {
    super(config, log, instrumentationEnabled);
    Multimap<String, String> policyBasedBlockedConnection = ArrayListMultimap.create();
    if (config.hasPath(ServiceConfigKeys.POLICY_BASED_BLOCKED_CONNECTION) &&
        config.getStringList(ServiceConfigKeys.POLICY_BASED_BLOCKED_CONNECTION).size() > 0 ){
      for(String sourceSinkPair:config.getStringList(ServiceConfigKeys.POLICY_BASED_BLOCKED_CONNECTION)){
        policyBasedBlockedConnection.put(sourceSinkPair.split(":")[0], sourceSinkPair.split(":")[1]);
      }
      this.optionalPolicyBasedBlockedConnection = Optional.of(policyBasedBlockedConnection);
    }
    else{
      this.optionalPolicyBasedBlockedConnection = Optional.absent();
    }

    if (config.hasPath(ServiceConfigKeys.POLICY_BASED_DATA_MOVEMENT_PATH) &&
        StringUtils.isNotBlank(config.getString(ServiceConfigKeys.POLICY_BASED_DATA_MOVEMENT_PATH))){
      optionalUserSpecifiedPath = Optional.of(config.getString(ServiceConfigKeys.POLICY_BASED_DATA_MOVEMENT_PATH));
    }
    else{
      optionalUserSpecifiedPath = Optional.absent();
    }

    if (config.hasPath(ServiceConfigKeys.TOPOLOGY_EDGE_WEIGHT_ENABLED)
        && StringUtils.isNotBlank(config.getString(ServiceConfigKeys.TOPOLOGY_EDGE_WEIGHT_ENABLED))) {
      edgeWeightEnabled = config.getString(ServiceConfigKeys.TOPOLOGY_EDGE_WEIGHT_ENABLED)
          .toLowerCase().equals("true") ? true : false;
    }
  }

  @Override
  public Map<Spec, SpecExecutor> compileFlow(Spec spec) {
    // A Map from JobSpec to SpecProducer
    // TODO: Understand when multiple enties are put into this map, how the execution of them being proceeded.
    Map<Spec, SpecExecutor> specExecutorInstanceMap = Maps.newLinkedHashMap();
    pathFinding(specExecutorInstanceMap, spec);
    return specExecutorInstanceMap;
  }

  /**
   * As the base implementation here, all templates will be considered for each edge.
   */
  @Override
  protected void populateEdgeTemplateMap() {
    for (FlowEdge flowEdge:this.weightedGraph.edgeSet()) {
      edgeTemplateMap.put(flowEdge.getEdgeIdentity(),
          templateCatalog.get().
              getAllTemplates().
              stream().map(jobTemplate -> jobTemplate.getUri()).collect(Collectors.toList()));
    }
  }

  /**
   * @return Transform a set of {@link TopologySpec} into a instance of {@link org.jgrapht.graph.WeightedMultigraph}
   * and filter out connections between blacklisted vertices that user specified.
   * The side-effect of this function only stays in memory, so each time a logical flow is compiled, the multigraph will
   * be re-calculated.
   *
   * TODO: Have finer granularity of edge filtering, which is removing a triplet of <SourceNode, targetNode, SpecExectorInstance>
   *   But this could involve more knowledge from users where they are required to specify SpecExecutor as well.
   */
  private void inMemoryWeightGraphGenerator(){
    for( TopologySpec topologySpec : topologySpecMap.values()) {
      weightGraphGenerateHelper(topologySpec);
    }

//    populateEdgeTemplateMap();

    // Filter out connection appearing in {@link optionalPolicyBasedBlockedConnection}
    if (optionalPolicyBasedBlockedConnection.isPresent()) {
      for (Map.Entry<String, String> singleBlacklistEntry:optionalPolicyBasedBlockedConnection.get().entries()){
        ServiceNode blockedNodeSrc = new BaseServiceNodeImpl(singleBlacklistEntry.getKey());
        ServiceNode blockedNodeDst = new BaseServiceNodeImpl(singleBlacklistEntry.getValue());
        if (weightedGraph.containsEdge(blockedNodeSrc, blockedNodeDst)){
          weightedGraph.removeAllEdges(blockedNodeSrc, blockedNodeDst);
          vertexSafeDeletionAttempt(blockedNodeSrc, weightedGraph);
          vertexSafeDeletionAttempt(blockedNodeSrc, weightedGraph);
        }
      }
    }
  }

  private void vertexSafeDeletionAttempt(ServiceNode node, WeightedMultigraph weightedGraph){
    if (weightedGraph.inDegreeOf(node) == 0 && weightedGraph.outDegreeOf(node) == 0){
      log.info("Node " + node.getNodeName() + " has no connection with it therefore delete it.");
      weightedGraph.removeVertex(node);
    }
  }

  // Basically a dijkstra path finding for connecting source and sink by multiple hops in between.
  // If there's any user-specified prioritization, conduct the DFS and see if the user-specified path is available.

  // TODO: It is expected to introduce stronger locking mechanism to ensure when pathFinding is going on
  // there's no updates on TopologySpec, or user should be aware of the possibility
  // that a topologySpec not being reflected in pathFinding.
  private void pathFinding(Map<Spec, SpecExecutor> specExecutorInstanceMap, Spec spec){
    inMemoryWeightGraphGenerator();
    FlowSpec flowSpec = (FlowSpec) spec;
    if (optionalUserSpecifiedPath.isPresent()) {
      log.info("Starting to evaluate user's specified path ... ");
      if (userSpecifiedPathVerificator(specExecutorInstanceMap, flowSpec)){
        log.info("User specified path[ " + optionalUserSpecifiedPath.get() + "] successfully verified.");
        return;
      }
      else {
        log.error("Will not execute user specified path[ " + optionalUserSpecifiedPath.get() + "]");
        log.info("Start to execute FlowCompiler's algorithm for valid data movement path");
      }
    }

    ServiceNode sourceNode = new BaseServiceNodeImpl(flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY));
    ServiceNode targetNode = new BaseServiceNodeImpl(flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY));
    List<FlowEdge> resultEdgePath = dijkstraBasedPathFindingHelper(sourceNode, targetNode);
    for (int i = 0 ; i < resultEdgePath.size() - 1 ; i ++ ) {
      FlowEdge tmpFlowEdge = resultEdgePath.get(i);
      ServiceNode edgeSrcNode = ((LoadBasedFlowEdgeImpl)tmpFlowEdge).getSourceNode();
      ServiceNode edgeTgtNode = ((LoadBasedFlowEdgeImpl)tmpFlowEdge).getTargetNode();
      specExecutorInstanceMap.put(jobSpecGenerator(edgeSrcNode, edgeTgtNode, flowSpec),
          ((LoadBasedFlowEdgeImpl)(resultEdgePath.get(i))).getSpecExecutorInstance());
    }
  }

  // Conduct dijkstra algorithm for finding shoretest path on WeightedDirectedMultiGraph.
  @VisibleForTesting
  public List<FlowEdge> dijkstraBasedPathFindingHelper(ServiceNode sourceNode, ServiceNode targetNode){
    DijkstraShortestPath<ServiceNode, FlowEdge> dijkstraShortestPath =
        new DijkstraShortestPath(this.weightedGraph, sourceNode, targetNode);
    return dijkstraShortestPath.getPathEdgeList();
  }

  // If path specified not existed, return false;
  // else return true.
  private boolean userSpecifiedPathVerificator(Map<Spec, SpecExecutor> specExecutorInstanceMap, FlowSpec flowSpec){
    Map<Spec, SpecExecutor> tmpSpecExecutorInstanceMap = new HashMap<>();
    List<String> userSpecfiedPath = Arrays.asList(optionalUserSpecifiedPath.get().split(","));
    for (int i = 0 ; i < userSpecfiedPath.size() - 1 ; i ++ ) {
      ServiceNode sourceNode = new BaseServiceNodeImpl(userSpecfiedPath.get(i));
      ServiceNode targetNode = new BaseServiceNodeImpl(userSpecfiedPath.get(i+1));
      if (weightedGraph.containsVertex(sourceNode) && weightedGraph.containsVertex(targetNode) &&
      weightedGraph.containsEdge(sourceNode, targetNode)) {
        tmpSpecExecutorInstanceMap.put(jobSpecGenerator(sourceNode, targetNode, flowSpec),
            (((LoadBasedFlowEdgeImpl)weightedGraph.getEdge(sourceNode, targetNode)).getSpecExecutorInstance()));
      }
      else {
        log.error("User Specified Path is invalid");
        return false;
      }
    }
    specExecutorInstanceMap.putAll(tmpSpecExecutorInstanceMap);
    return true;
  }

  // Helper function for transform TopologySpecMap into a weightedDirectedGraph.
  private void weightGraphGenerateHelper(TopologySpec topologySpec){
    try{
      Map<ServiceNode, ServiceNode> capabilities =
          topologySpec.getSpecExecutor().getCapabilities().get();
      for (Map.Entry<ServiceNode, ServiceNode> capability : capabilities.entrySet()) {
        ServiceNode sourceNode = capability.getKey();
        ServiceNode targetNode = capability.getValue();
        if (!weightedGraph.containsVertex(sourceNode)){
          weightedGraph.addVertex(sourceNode);
        }
        if (!weightedGraph.containsVertex(targetNode)){
          weightedGraph.addVertex(targetNode);
        }

        FlowEdge flowEdge = new LoadBasedFlowEdgeImpl
            (sourceNode, targetNode, defaultFlowEdgeProps, topologySpec.getSpecExecutor());

        // In Multi-Graph if flowEdge existed, just skip it.
        if (!weightedGraph.containsEdge(flowEdge)) {
          weightedGraph.addEdge(sourceNode, targetNode, flowEdge);
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      Instrumented.markMeter(this.flowCompilationFailedMeter);
      throw new RuntimeException("Cannot determine topology capabilities", e);
    }
  }

  /**
   * Generate JobSpec based on the #templateURI that user specified.
   */
  private JobSpec jobSpecGenerator(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdge flowEdge, URI templateURI, FlowSpec flowSpec){
    JobSpec jobSpec;
    JobSpec.Builder jobSpecBuilder = JobSpec.builder(jobSepcURIGenerator(flowSpec, sourceNode, targetNode))
        .withConfig(flowSpec.getConfig())
        .withDescription(flowSpec.getDescription())
        .withVersion(flowSpec.getVersion());
    if (edgeTemplateMap.containsKey(flowEdge.getEdgeIdentity())
        && edgeTemplateMap.get(flowEdge.getEdgeIdentity()).contains(templateURI)){
      jobSpecBuilder.withTemplate(templateURI);
      try{
        jobSpec = new ResolvedJobSpec(jobSpecBuilder.build(), templateCatalog.get());
        log.info("Resolved JobSpec properties are: " + jobSpec.getConfigAsProperties());
      }catch (SpecNotFoundException | JobTemplate.TemplateException e) {
        throw new RuntimeException("Could not resolve template in JobSpec from TemplateCatalog", e);
      }
    }
    else {
      jobSpec = jobSpecBuilder.build();
      log.info("Unresolved JobSpec properties are: " + jobSpec.getConfigAsProperties());
    }
    return jobSpec;
  }

  /**
   * A naive implementation of resolving templates in each JobSpec among Multi-hop FlowSpec.
   * Handling the case when edge is not specified.
   * Select the first available template.
   * Set as the default invocation.
   *
   * TODO: Need to define how {@link #edgeTemplateMap} is initializd.
   */
  private JobSpec jobSpecGenerator(ServiceNode sourceNode, ServiceNode targetNode, FlowSpec flowSpec){
    FlowEdge flowEdge = weightedGraph.getAllEdges(sourceNode, targetNode).iterator().next();
    URI firstTemplateURI = (edgeTemplateMap!=null && edgeTemplateMap.containsKey(flowEdge.getEdgeIdentity())) ?
        edgeTemplateMap.get(flowEdge.getEdgeIdentity()).get(0)
        : jobSpecGenerator(flowSpec).getUri();
    return this.jobSpecGenerator(sourceNode, targetNode, flowEdge, firstTemplateURI, flowSpec);
  }

  /**
   * A naive implementation of generating a jobSpec's URI within a multi-hop logicial Flow.
   */
  public static URI jobSepcURIGenerator(FlowSpec flowSpec, ServiceNode sourceNode, ServiceNode targetNode) {
    try {
      return new URI(flowSpec.getUri().getScheme(), flowSpec.getUri().getAuthority(),
          "/" + sourceNode.getNodeName() + "-" + targetNode.getNodeName(), null);
    } catch (URISyntaxException e){
      log.error("URI construction failed when jobSpec from " + sourceNode.getNodeName() + " to " + targetNode.getNodeName());
      throw new RuntimeException();
    }
  }
}
