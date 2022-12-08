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

import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphMonitor;
import org.apache.hadoop.fs.Path;


import org.apache.gobblin.configuration.ConfigurationKeys;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template_catalog.UpdatableFSFlowTemplateCatalog;
import org.apache.gobblin.service.monitoring.GitFlowGraphMonitor;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.pathfinder.PathFinder;
import org.apache.gobblin.service.modules.restli.FlowConfigUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.template_catalog.ObservingFSFlowEdgeTemplateCatalog;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;


/***
 * Take in a logical {@link Spec} ie flow and compile corresponding materialized job {@link Spec}
 * and its mapping to {@link SpecExecutor}.
 */
@Alpha
@Slf4j
public class MultiHopFlowCompiler extends BaseFlowToJobSpecCompiler {
  private AtomicReference<FlowGraph> flowGraph;
  @Getter
  private ServiceManager serviceManager;
  @Getter
  private CountDownLatch initComplete = new CountDownLatch(1);

  private FlowGraphMonitor flowGraphMonitor;

  private ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  private DataMovementAuthorizer dataMovementAuthorizer;

  private Map<String, String> dataNodeAliasMap = new HashMap<>();

  // a map to hold aliases of data nodes, e.g. gobblin.service.datanode.aliases.map=node1-dev:node1,node1-stg:node1,node1-prod:node1
  public static final String DATA_NODE_ID_TO_ALIAS_MAP = ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX + "datanode.aliases.map";

  public MultiHopFlowCompiler(Config config) {
    this(config, true);
  }

  public MultiHopFlowCompiler(Config config, boolean instrumentationEnabled) {
    this(config, Optional.<Logger>absent(), instrumentationEnabled);
  }

  public MultiHopFlowCompiler(Config config, Optional<Logger> log) {
    this(config, log, true);
  }

  public MultiHopFlowCompiler(Config config, AtomicReference<FlowGraph> flowGraph) {
    super(config, Optional.absent(), true);
    this.flowGraph = flowGraph;
    this.dataMovementAuthorizer = new NoopDataMovementAuthorizer(config);
  }

  public MultiHopFlowCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled) {
    super(config, log, instrumentationEnabled);
    try {
      this.dataNodeAliasMap = config.hasPath(DATA_NODE_ID_TO_ALIAS_MAP)
          ? Splitter.on(",").withKeyValueSeparator(":").split(config.getString(DATA_NODE_ID_TO_ALIAS_MAP))
          : new HashMap<>();
    } catch (RuntimeException e) {
      MultiHopFlowCompiler.log.warn("Exception reading data node alias map, ignoring it.", e);
    }
    // Use atomic reference to avoid partial flowgraph upgrades during path compilation.
    this.flowGraph = new AtomicReference<>(new BaseFlowGraph(dataNodeAliasMap));

    Optional<? extends UpdatableFSFlowTemplateCatalog> flowTemplateCatalog;
    if (config.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)
        && StringUtils.isNotBlank(config.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY))) {

      try {
        String flowTemplateCatalogClassName = ConfigUtils.getString(this.config, ServiceConfigKeys.TEMPLATE_CATALOGS_CLASS_KEY, ObservingFSFlowEdgeTemplateCatalog.class.getCanonicalName());
        flowTemplateCatalog = Optional.of(
            (UpdatableFSFlowTemplateCatalog) ConstructorUtils.invokeConstructor(Class.forName(new ClassAliasResolver<>(UpdatableFSFlowTemplateCatalog.class)
                .resolve(flowTemplateCatalogClassName)), config, rwLock));
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
        throw new RuntimeException("Cannot instantiate " + getClass().getName(), e);
      }
    } else {
      return;
    }
    Config gitFlowGraphConfig = this.config;
    if (this.config.hasPath(ConfigurationKeys.ENCRYPT_KEY_LOC)) {
      //Add encrypt.key.loc config to the config passed to GitFlowGraphMonitor
      gitFlowGraphConfig = this.config
          .withValue(GitFlowGraphMonitor.GIT_FLOWGRAPH_MONITOR_PREFIX + "." + ConfigurationKeys.ENCRYPT_KEY_LOC, config.getValue(ConfigurationKeys.ENCRYPT_KEY_LOC));
    }

    try {
      String dataMovementAuthorizerClassName = ConfigUtils.getString(this.config, ServiceConfigKeys.DATA_MOVEMENT_AUTHORIZER_CLASS,
          NoopDataMovementAuthorizer.class.getCanonicalName());
      this.dataMovementAuthorizer = (DataMovementAuthorizer) ConstructorUtils.invokeConstructor(Class.forName(new ClassAliasResolver<>(DataMovementAuthorizer.class).resolve(dataMovementAuthorizerClassName)), this.config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      String flowGraphMonitorClassName = ConfigUtils.getString(this.config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOWGRAPH_CLASS_KEY, GitFlowGraphMonitor.class.getCanonicalName());
      this.flowGraphMonitor = (FlowGraphMonitor) ConstructorUtils.invokeConstructor(Class.forName(new ClassAliasResolver<>(FlowGraphMonitor.class).resolve(
        flowGraphMonitorClassName)), gitFlowGraphConfig, flowTemplateCatalog, this, this.topologySpecMap, this.getInitComplete(), instrumentationEnabled);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.serviceManager = (flowTemplateCatalog.isPresent() && flowTemplateCatalog.get() instanceof ObservingFSFlowEdgeTemplateCatalog) ?
       new ServiceManager(Lists.newArrayList(this.flowGraphMonitor, flowTemplateCatalog.get())) : new ServiceManager(Lists.newArrayList(this.flowGraphMonitor));

    addShutdownHook();
    //Start the git flow graph monitor
    try {
      this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      MultiHopFlowCompiler.log.error("Timed out while waiting for the service manager to start up", te);
      throw new RuntimeException(te);
    }
  }

  /**
   * Mark the {@link SpecCompiler} as active. This in turn activates the {@link GitFlowGraphMonitor}, allowing to start polling
   * and processing changes
   * @param active
   */
  @Override
  public void setActive(boolean active) {
    super.setActive(active);
    if (this.flowGraphMonitor != null) {
      this.flowGraphMonitor.setActive(active);
    }
  }

  @Override
  public void awaitHealthy() throws InterruptedException {
    if (this.getInitComplete().getCount() > 0) {
      log.info("Waiting for the MultiHopFlowCompiler to become healthy..");
      this.getInitComplete().await();
      log.info("The MultihopFlowCompiler is healthy and ready to orchestrate flows.");
    }
    return;
  }

  /**
   * j
   * @param spec an instance of {@link FlowSpec}.
   * @return A DAG of {@link JobExecutionPlan}s, which encapsulates the compiled {@link org.apache.gobblin.runtime.api.JobSpec}s
   * together with the {@link SpecExecutor} where the job can be executed.
   */
  @Override
  public Dag<JobExecutionPlan> compileFlow(Spec spec) {
    Preconditions.checkNotNull(spec);
    Preconditions.checkArgument(spec instanceof FlowSpec, "MultiHopFlowCompiler only accepts FlowSpecs");
    FlowGraph graph = this.flowGraph.get();
    long startTime = System.nanoTime();

    FlowSpec flowSpec = (FlowSpec) spec;
    String source = FlowConfigUtils.getDataNode(flowSpec.getConfig(), ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, this.dataNodeAliasMap);
    String destination = FlowConfigUtils.getDataNode(flowSpec.getConfig(), ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, this.dataNodeAliasMap);

    DataNode sourceNode = graph.getNode(source);
    if (sourceNode == null) {
      flowSpec.addCompilationError(source, destination, String.format("Flowgraph does not have a node with id %s", source));
      return null;
    }
    List<String> destNodeIds = FlowConfigUtils.getDataNodes(flowSpec.getConfig(), ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, this.dataNodeAliasMap);
    List<DataNode> destNodes = destNodeIds.stream().map(graph::getNode).collect(Collectors.toList());
    if (destNodes.contains(null)) {
      flowSpec.addCompilationError(source, destination, String.format("Flowgraph does not have a node with id %s", destNodeIds.get(destNodes.indexOf(null))));
      return null;
    }
    log.info(String.format("Compiling flow for source: %s and destination: %s", source, destination));

    List<FlowSpec> flowSpecs = splitFlowSpec(flowSpec);
    Dag<JobExecutionPlan> jobExecutionPlanDag = new Dag<>(new ArrayList<>());
    try {
      this.rwLock.readLock().lock();
      for (FlowSpec datasetFlowSpec : flowSpecs) {
        for (DataNode destNode : destNodes) {
          long authStartTime = System.nanoTime();
          try {
            boolean authorized = this.dataMovementAuthorizer.isMovementAuthorized(flowSpec, sourceNode, destNode);
            Instrumented.updateTimer(dataAuthorizationTimer, System.nanoTime() - authStartTime, TimeUnit.NANOSECONDS);
            if (!authorized) {
              String message = String.format("Data movement is not authorized for flow: %s, source: %s, destination: %s",
                  flowSpec.getUri().toString(), source, destination);
              log.error(message);
              datasetFlowSpec.addCompilationError(source, destination, message);
              return null;
            }
          } catch (Exception e) {
            Instrumented.markMeter(flowCompilationFailedMeter);
            datasetFlowSpec.addCompilationError(source, destination, Throwables.getStackTraceAsString(e));
            return null;
          }
        }

        //Compute the path from source to destination.
        FlowGraphPath flowGraphPath = graph.findPath(datasetFlowSpec);
        if (flowGraphPath != null) {
          //Convert the path into a Dag of JobExecutionPlans.
          jobExecutionPlanDag = jobExecutionPlanDag.merge(flowGraphPath.asDag(this.config));
        }
      }

      if (jobExecutionPlanDag.isEmpty()) {
        Instrumented.markMeter(flowCompilationFailedMeter);
        String message = String.format("No path found from source: %s and destination: %s", source, destination);
        log.info(message);

        if (!flowSpec.getCompilationErrors().stream().anyMatch(compilationError -> compilationError.errorPriority == 0)) {
          flowSpec.addCompilationError(source, destination, message);
        }
        return null;
      }
    } catch (PathFinder.PathFinderException | SpecNotFoundException | JobTemplate.TemplateException | URISyntaxException | ReflectiveOperationException e) {
      Instrumented.markMeter(flowCompilationFailedMeter);
      String message = String.format("Exception encountered while compiling flow for source: %s and destination: %s, %s", source, destination, Throwables.getStackTraceAsString(e));
      log.error(message, e);
      flowSpec.addCompilationError(source, destination, message);
      return null;
    } finally {
      this.rwLock.readLock().unlock();
    }
    Instrumented.markMeter(flowCompilationSuccessFulMeter);
    Instrumented.updateTimer(flowCompilationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    if (Boolean.parseBoolean(flowSpec.getConfigAsProperties().getProperty(ServiceConfigKeys.GOBBLIN_SERVICE_ADHOC_FLOW))) {
      for (Dag.DagNode<JobExecutionPlan> dagNode : jobExecutionPlanDag.getStartNodes()) {
        dagNode.getValue().getJobSpec().getConfigAsProperties().setProperty(ServiceConfigKeys.GOBBLIN_SERVICE_ADHOC_FLOW, "true");
      }
    }

    return jobExecutionPlanDag;
  }

  public void setFlowGraph(FlowGraph flowGraph) {
    this.flowGraph.set(flowGraph);
  }

  /**
   * If {@link FlowSpec} has {@link ConfigurationKeys#DATASET_SUBPATHS_KEY}, split it into multiple flowSpecs using a
   * provided base input and base output path to generate multiple source/destination paths.
   */
  private static List<FlowSpec> splitFlowSpec(FlowSpec flowSpec) {
    long flowExecutionId = FlowUtils.getOrCreateFlowExecutionId(flowSpec);
    List<FlowSpec> flowSpecs = new ArrayList<>();
    Config flowConfig = flowSpec.getConfig();

    if (flowConfig.hasPath(ConfigurationKeys.DATASET_SUBPATHS_KEY)) {
      List<String> datasetSubpaths = ConfigUtils.getStringList(flowConfig, ConfigurationKeys.DATASET_SUBPATHS_KEY);
      String baseInputPath = ConfigUtils.getString(flowConfig, ConfigurationKeys.DATASET_BASE_INPUT_PATH_KEY, "/");
      String baseOutputPath = ConfigUtils.getString(flowConfig, ConfigurationKeys.DATASET_BASE_OUTPUT_PATH_KEY, "/");

      if (ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.DATASET_COMBINE_KEY, false)) {
        Config newConfig = flowConfig.withoutPath(ConfigurationKeys.DATASET_SUBPATHS_KEY)
            .withValue(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX + "." + DatasetDescriptorConfigKeys.PATH_KEY,
                ConfigValueFactory.fromAnyRef(baseInputPath))
            .withValue(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX + "." + DatasetDescriptorConfigKeys.PATH_KEY,
                ConfigValueFactory.fromAnyRef(baseOutputPath))
            .withValue(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX + ".subPaths",
                ConfigValueFactory.fromAnyRef(convertStringListToGlobPattern(datasetSubpaths)))
            .withValue(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX + ".subPaths",
                ConfigValueFactory.fromAnyRef(convertStringListToGlobPattern(datasetSubpaths)));
        flowSpecs.add(copyFlowSpecWithNewConfig(flowSpec, newConfig));
      } else {
        for (String subPath : datasetSubpaths) {
          Config newConfig = flowConfig.withoutPath(ConfigurationKeys.DATASET_SUBPATHS_KEY)
              .withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId))
              .withValue(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX + "." + DatasetDescriptorConfigKeys.PATH_KEY,
                  ConfigValueFactory.fromAnyRef(new Path(baseInputPath, subPath).toString()))
              .withValue(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX + "." + DatasetDescriptorConfigKeys.PATH_KEY,
                  ConfigValueFactory.fromAnyRef(new Path(baseOutputPath, subPath).toString()));
          flowSpecs.add(copyFlowSpecWithNewConfig(flowSpec, newConfig));
        }
      }
    } else {
      flowSpecs.add(flowSpec);
    }

    return flowSpecs;
  }

  /**
   * Convert string list to string pattern that will work for globs.
   *
   * e.g. ["test1", "test2", test3"] -> "{test1,test2,test}"
   */
  private static String convertStringListToGlobPattern(List<String> stringList) {
    return "{" + Joiner.on(",").join(stringList) + "}";
  }

  private static FlowSpec copyFlowSpecWithNewConfig(FlowSpec flowSpec, Config newConfig) {
    FlowSpec.Builder builder = FlowSpec.builder(flowSpec.getUri()).withVersion(flowSpec.getVersion())
        .withDescription(flowSpec.getDescription()).withConfig(newConfig);

    if (flowSpec.getTemplateURIs().isPresent()) {
      builder = builder.withTemplates(flowSpec.getTemplateURIs().get());
    }

    if (flowSpec.getChildSpecs().isPresent()) {
      builder = builder.withTemplates(flowSpec.getChildSpecs().get());
    }

    return builder.build();
  }

  /**
   * Register a shutdown hook for this thread.
   */
  private void addShutdownHook() {
    ServiceManager manager = this.serviceManager;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        // Give the services 5 seconds to stop to ensure that we are responsive to shutdown
        // requests.
        try {
          manager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
        } catch (TimeoutException timeout) {
          // stopping timed out
        }
      }
    });
  }
}
