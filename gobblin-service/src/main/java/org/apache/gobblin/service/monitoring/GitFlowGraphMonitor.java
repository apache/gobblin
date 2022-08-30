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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphMonitor;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
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
  private static final String PROPERTIES_EXTENSIONS = "properties";
  private static final String CONF_EXTENSIONS = "conf";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR = "git-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME = "master";
  static final String SHOULD_CHECKPOINT_HASHES = "shouldCheckpointHashes";

  private static final int DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL = 60;

  private static final Config DEFAULT_FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
      .put(ConfigurationKeys.GIT_MONITOR_REPO_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR)
      .put(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
      .put(ConfigurationKeys.GIT_MONITOR_BRANCH_NAME, DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME)
      .put(ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL)
      .put(ConfigurationKeys.JAVA_PROPS_EXTENSIONS, PROPERTIES_EXTENSIONS)
      .put(ConfigurationKeys.HOCON_FILE_EXTENSIONS, CONF_EXTENSIONS)
      .put(SHOULD_CHECKPOINT_HASHES, false)
      .build());

  private Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog;
  private final CountDownLatch initComplete;

  public GitFlowGraphMonitor(Config config, Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      FlowGraph graph, Map<URI, TopologySpec> topologySpecMap, CountDownLatch initComplete) {
    super(config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK));
    Config configWithFallbacks = config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK);
    this.flowTemplateCatalog = flowTemplateCatalog;
    this.initComplete = initComplete;
    this.listeners.add(new GitFlowGraphListener(
        flowTemplateCatalog, graph, topologySpecMap, configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_REPO_DIR),
        configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR), configWithFallbacks.getString(ConfigurationKeys.JAVA_PROPS_EXTENSIONS),
        configWithFallbacks.getString(ConfigurationKeys.HOCON_FILE_EXTENSIONS))
    );
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
   * Sort the changes in a commit so that changes to node files appear before changes to edge files. This is done so that
   * node related changes are applied to the FlowGraph before edge related changes. An example where the order matters
   * is the case when a commit adds a new node n2 as well as adds an edge from an existing node n1 to n2. To ensure that the
   * addition of edge n1->n2 is successful, node n2 must exist in the graph and so needs to be added first. For deletions,
   * the order does not matter and ordering the changes in the commit will result in the same FlowGraph state as if the changes
   * were unordered. In other words, deletion of a node deletes all its incident edges from the FlowGraph. So processing an
   * edge deletion later results in a no-op. Note that node and edge files do not change depth in case of modifications.
   *
   * If there are multiple commits between successive polls to Git, the re-ordering of changes across commits should not
   * affect the final state of the FlowGraph. This is because, the order of changes for a given file type (i.e. node or edge)
   * is preserved.
   */
  @Override
  void processGitConfigChanges() throws GitAPIException, IOException {
    if (flowTemplateCatalog.isPresent() && flowTemplateCatalog.get().getAndSetShouldRefreshFlowGraph(false)) {
      log.info("Change to template catalog detected, refreshing FlowGraph");
      this.gitRepo.initRepository();
    }

    List<DiffEntry> changes = this.gitRepo.getChanges();
    Collections.sort(changes, new GitFlowgraphComparator());
    processGitConfigChangesHelper(changes);
    //Decrements the latch count. The countdown latch is initialized to 1. So after the first time the latch is decremented,
    // the following operation should be a no-op.
    this.initComplete.countDown();
  }

  class GitFlowgraphComparator implements Comparator<DiffEntry> {
    public int compare(DiffEntry o1, DiffEntry o2) {
      Integer o1Depth = (o1.getNewPath() != null) ? (new Path(o1.getNewPath())).depth() : (new Path(o1.getOldPath())).depth();
      Integer o2Depth = (o2.getNewPath() != null) ? (new Path(o2.getNewPath())).depth() : (new Path(o2.getOldPath())).depth();
      return o1Depth.compareTo(o2Depth);
    }
  }

}
