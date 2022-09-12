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

import java.net.URI;
import java.util.Map;

import org.eclipse.jgit.diff.DiffEntry;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraphListener;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;


/**
 * Listener for {@link GitFlowGraphMonitor} to apply changes from Git to a {@link FlowGraph}
 */
public class GitFlowGraphListener extends BaseFlowGraphListener implements GitDiffListener {

  public GitFlowGraphListener(Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      FlowGraph graph, Map<URI, TopologySpec> topologySpecMap, String baseDirectory, String folderName, String javaPropsExtentions,
      String hoconFileExtentions) {
    super(flowTemplateCatalog, graph, topologySpecMap, baseDirectory, folderName, javaPropsExtentions, hoconFileExtentions);

  }

  /**
   * Add an element (i.e., a {@link DataNode}, or a {@link FlowEdge} to
   * the {@link FlowGraph} for an added, updated or modified node or edge file.
   * @param change
   */
  @Override
  public void addChange(DiffEntry change) {
    Path path = new Path(change.getNewPath());
    if (path.depth() == NODE_FILE_DEPTH) {
      addDataNode(change.getNewPath());
    } else if (path.depth() == EDGE_FILE_DEPTH) {
      addFlowEdge(change.getNewPath());
    }
  }

  /**
   * Remove an element (i.e. either a {@link DataNode} or a {@link FlowEdge} from the {@link FlowGraph} for
   * a renamed or deleted {@link DataNode} or {@link FlowEdge} file.
   * @param change
   */
  @Override
  public void removeChange(DiffEntry change) {
    Path path = new Path(change.getOldPath());
    if (path.depth() == NODE_FILE_DEPTH) {
      removeDataNode(change.getOldPath());
    } else if (path.depth() == EDGE_FILE_DEPTH) {
      removeFlowEdge(change.getOldPath());
    }
  }
}
