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

import gobblin.runtime.api.FlowSpec;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;

import gobblin.instrumented.Instrumented;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.runtime.api.TopologySpec;
import gobblin.service.ServiceConfigKeys;

// Users are capable to inject hints/prioritization into route selection, in two forms:
// 1. PolicyBasedBlockedConnection: Define some undesired routes
// 2. Specified a complete path. FlowCompiler is responsible to verify if the path given is valid.

public class MultiHopsFlowToJobSpecCompiler extends BaseFlowToJobSpecCompiler {

  /* TODO: When the connection between hops is no longer configuration based,
           should ensure the updates of following two data structures atomic with TopologySpec Catagory updates */
  // Inverted index for specExecutor: Given a pair of source and sink in the format of "source-string"
  // Users are free to inject Prioritization criteria in the PriorityQueue.
  private Map<String, PriorityQueue<SpecExecutorInstanceProducer>> specExecutorInvertedIndex = new HashMap<>();
  // Adjacency List to indicate connection between different hops, for convenience of pathFinding.
  private Map<String, List<String>> adjacencyList = new HashMap<>();

  //Contains the user-specified connection that are not desired to appear in JobSpec.
  //It can be used for avoiding known expensive or undesired data movement.
  private Optional<Multimap<String, String>> optionalPolicyBasedBlockedConnection;
  // Contains user-specified complete path of how the data movement is executed from source to sink.
  private Optional<String> optionalUserSpecifiedPath;

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
  }

  @Override
  public Map<Spec, SpecExecutorInstanceProducer> compileFlow(Spec spec) {
    // A Map from JobSpec to SpecExecutorInstanceProducer
    // TODO: Understand when multiple enties are put into this map, how the execution of them being proceeded.
    Map<Spec, SpecExecutorInstanceProducer> specExecutorInstanceMap = Maps.newLinkedHashMap();
    pathFinding(specExecutorInstanceMap, spec);
    return specExecutorInstanceMap;
  }

  /**
   * @param topologySpecMap Contains all topologySpec that currently existed
   * @return Transform all topologySpec into a adjacency list for convenience of path finding.
   *         A path here represents a complete route from source to sink, with possibly multiple hops in the between.
   * This function is invoked right before the adjacency list is to be used, ensuring up-to-date connection info.
   */
  private void inMemoryAdjacencyListAndInvertedIndexGenerator(Map<URI, TopologySpec> topologySpecMap){
    for( TopologySpec topologySpec : topologySpecMap.values()) {
      adjacencyListAndInvertedListGenerateHelper(topologySpec);
    }

    // Filter out connection appearing in {@link optionalPolicyBasedBlockedConnection}
    if (optionalPolicyBasedBlockedConnection.isPresent()) {
      for (Map.Entry<String, String> singleBlacklistEntry:optionalPolicyBasedBlockedConnection.get().entries()){
        String blockedConnnectionKey = singleBlacklistEntry.getKey();
        String blockedConnectionValue = singleBlacklistEntry.getValue();
        if (adjacencyList.get(blockedConnnectionKey).contains(blockedConnectionValue)) {
          adjacencyList.get(blockedConnnectionKey).remove(blockedConnectionValue);
          if (adjacencyList.get(blockedConnnectionKey).size() == 0){
            adjacencyList.remove(blockedConnnectionKey);
          }
        }
      }
    }
  }

  // Basically a depth-first-search for connecting source and sink by multiple hops in between.
  // If there's any user-specified prioritization, conduct the DFS and see if the user-specified path is available.

  // TODO: It is expected to introduce stronger locking mechanism to ensure when pathFinding is going on
  // there's no updates on TopologySpec, or user should be aware of the possibility
  // that a topologySpec not being reflected in pathFinding.
  private void pathFinding(Map<Spec, SpecExecutorInstanceProducer> specExecutorInstanceMap, Spec spec){
    inMemoryAdjacencyListAndInvertedIndexGenerator(topologySpecMap);
    if (optionalUserSpecifiedPath.isPresent()) {
      log.info("Starting to evaluate user's specified path ... ");
      if (userSpecifiedPathVerificator(specExecutorInstanceMap, spec)){
        log.info("User specified path[ " + optionalUserSpecifiedPath.get() + "] successfully verified.");
        return;
      }
      else {
        log.error("Will not execute user specified path[ " + optionalUserSpecifiedPath.get());

      }
    }
    else {
      Set<String> visited = new HashSet();
      List<String> resultPath = new ArrayList<>();
      FlowSpec flowSpec = (FlowSpec) spec;
      String source = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY);
      String destination = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY);
      pathFindingHelper(source, destination, resultPath, visited);

      for (int i = 0 ; i < resultPath.size() - 1 ; i ++ ) {
        specExecutorInstanceMap.put(jobSpecGenerator(flowSpec),
            specExecutorInvertedIndex.get(keyGenerationHelper(resultPath.get(i), resultPath.get(i+1))).peek());
      }
    }
  }

  private void pathFindingHelper(String source, String sink, List<String> resultPath, Set<String> visited) {
    if ( resultPath.get(resultPath.size() - 1).equals(sink)){
      return;
    }
    else{
      resultPath.add(source);
      visited.add(source);
      for(String adjacentNode: this.adjacencyList.get(source)) {
        if (!visited.contains(adjacentNode)){
          pathFindingHelper(adjacentNode, sink, resultPath, visited);
        }
      }
    }
  }

  // If path specified not existed, return false;
  // else return true.
  private boolean userSpecifiedPathVerificator(Map<Spec, SpecExecutorInstanceProducer> specExecutorInstanceMap, Spec spec){
    Map<Spec, SpecExecutorInstanceProducer> tmpSpecExecutorInstanceMap = new HashMap<>();
    List<String> userSpecfiedPath = Arrays.asList(optionalUserSpecifiedPath.get().split(","));
    for (int i = 0 ; i < userSpecfiedPath.size() - 1 ; i ++ ) {
      if (adjacencyList.get(userSpecfiedPath.get(i)).contains(userSpecfiedPath.get(i + 1)) &&
          specExecutorInvertedIndex.containsKey(keyGenerationHelper(userSpecfiedPath.get(i), userSpecfiedPath.get(i + 1)))) {
        tmpSpecExecutorInstanceMap.put(jobSpecGenerator((FlowSpec) spec),
            specExecutorInvertedIndex.get(keyGenerationHelper(userSpecfiedPath.get(i), userSpecfiedPath.get(i + 1))).peek());
      }
      else {
        log.error("User Specified Path is invalid");
        return false;
      }
    }
    specExecutorInstanceMap.putAll(tmpSpecExecutorInstanceMap);
    return true;
  }

  // Helper functions that will be invoked every time a new topologySpec's information is digested to
  // update AdjacencyList and InvertedList.
  private void adjacencyListAndInvertedListGenerateHelper(TopologySpec topologySpec){
    try{
      Map<String, String> capabilities =
          (Map<String, String>) topologySpec.getSpecExecutorInstanceProducer().getCapabilities().get();
      for (Map.Entry<String, String> capability : capabilities.entrySet()) {

        if (adjacencyList.containsKey(capability.getKey())){
          adjacencyList.get(capability.getKey()).add(capability.getValue());
        }
        else{
          adjacencyList.put(capability.getKey(), Arrays.asList(capability.getValue()));
        }

        if (specExecutorInvertedIndex.containsKey(keyGenerationHelper(capability.getKey(),capability.getValue()))){
          specExecutorInvertedIndex.get(keyGenerationHelper(capability.getKey(),capability.getValue()))
              .add(topologySpec.getSpecExecutorInstanceProducer());
        }
        else {
          PriorityQueue<SpecExecutorInstanceProducer> pq =
              new PriorityQueue<>(new SpecExecutorInstanceProducerComparator());
          pq.add(topologySpec.getSpecExecutorInstanceProducer());
          specExecutorInvertedIndex.put(keyGenerationHelper(capability.getKey(),capability.getValue()), pq);
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      Instrumented.markMeter(this.flowCompilationFailedMeter);
      throw new RuntimeException("Cannot determine topology capabilities", e);
    }
  }

  // Helper function for generating key in invertedList for query SpecExecutorInstance using pair of source and sink.
  private String keyGenerationHelper(String source, String sink){
    return source + "-" + sink;
  }

  private class SpecExecutorInstanceProducerComparator implements Comparator<SpecExecutorInstanceProducer> {
    public int compare(SpecExecutorInstanceProducer p1, SpecExecutorInstanceProducer p2) {
      if (config.hasPath(ServiceConfigKeys.POLICY_BASED_SPEC_EXECUTOR_SELECTION)) {
        // TODO: Add policies here to handle the case when there are multiple SpecExecutors having the same capabilities
        // Switch ():
        return 1;
      }
      // If no policy specified simply favor the former one.
      else return 1;
    }
  }
}
