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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flow.MultiHopFlowCompiler;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraphHelper;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphMonitor;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;


/**
 * Service that monitors for changes to {@link org.apache.gobblin.service.modules.flowgraph.FlowGraph} from a git repository.
 * The git repository must have an inital commit that has no files since that is used as a base for getting
 * the change list.
 * The {@link DataNode}s and {@link FlowEdge}s in FlowGraph need to be organized with the following directory structure on git:
 * <root_flowGraph_dir>/<nodeName>/<nodeName>.properties
 * <root_flowGraph_dir>/<nodeName1>/<nodeName2>/<edgeName>.properties
 */
@Slf4j
public class GitFlowGraphMonitor extends GitMonitoringService implements FlowGraphMonitor {
  public static final String GIT_FLOWGRAPH_MONITOR_PREFIX = "gobblin.service.gitFlowGraphMonitor";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR = "git-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME = "master";
  static final String SHOULD_CHECKPOINT_HASHES = "shouldCheckpointHashes";

  private static final int DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL = 60;

  private static final Config DEFAULT_FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
      .put(ConfigurationKeys.GIT_MONITOR_REPO_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR)
      .put(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
      .put(ConfigurationKeys.GIT_MONITOR_BRANCH_NAME, DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME)
      .put(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS, ConfigurationKeys.DEFAULT_PROPERTIES_EXTENSIONS)
      .put(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS, ConfigurationKeys.DEFAULT_CONF_EXTENSIONS)
      .put(ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL)
      .put(SHOULD_CHECKPOINT_HASHES, false).build());

  private final Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog;
  private final CountDownLatch initComplete;
  private final BaseFlowGraphHelper flowGraphHelper;
  private final MultiHopFlowCompiler multihopFlowCompiler;

  public GitFlowGraphMonitor(Config config, Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog, MultiHopFlowCompiler compiler
      , Map<URI, TopologySpec> topologySpecMap, CountDownLatch initComplete, boolean instrumentationEnabled) {
    super(config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK));
    Config configWithFallbacks = config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK);
    this.flowTemplateCatalog = flowTemplateCatalog;
    this.initComplete = initComplete;
    this.flowGraphHelper = new BaseFlowGraphHelper(flowTemplateCatalog, topologySpecMap, configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_REPO_DIR),
        configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR), configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS),
        configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS), instrumentationEnabled, config);
    this.multihopFlowCompiler = compiler;
  }

  /**
   * Determine if the service should poll Git. Current behavior is both leaders and followers(s) will poll Git for
   * changes to {@link FlowGraph}.
   */
  @Override
  public boolean shouldPollGit() {
    return this.isActive;
  }

  /**
   * Reprocesses the entire flowgraph from the root folder every time a change in git is detected
   */
  @Override
  void processGitConfigChanges()
      throws GitAPIException, IOException {
    // Pulls repository to latest and grabs changes
    List<DiffEntry> changes = this.gitRepo.getChanges();
    if (flowTemplateCatalog.isPresent() && flowTemplateCatalog.get().getAndSetShouldRefreshFlowGraph(false)) {
      log.info("Change to template catalog detected, refreshing FlowGraph");
      this.gitRepo.initRepository();
    } else if (changes.isEmpty()) {
      return;
    }
    log.info("Detected changes in flowGraph, refreshing Flowgraph");

    FlowGraph newGraph = this.flowGraphHelper.generateFlowGraph();
    if (newGraph != null) {
      this.multihopFlowCompiler.setFlowGraph(newGraph);
    }
    // Noop if flowgraph is already initialized
    this.initComplete.countDown();
    this.gitRepo.moveCheckpointAndHashesForward();
  }
}
