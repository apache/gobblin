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

package org.apache.gobblin.service.modules.flowgraph;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.diff.DiffEntry;

import org.apache.gobblin.service.monitoring.GitDiffListener;
import org.apache.gobblin.service.monitoring.GitFlowGraphMonitor;


/**
 * Listener for {@link GitFlowGraphMonitor} to apply changes from Git to a {@link FlowGraph}
 */
public class GitFlowGraphListener implements GitDiffListener {

  private final AtomicReference<FlowGraph> flowGraph;
  private final BaseFlowGraphHelper baseFlowGraphHelper;

  public GitFlowGraphListener(AtomicReference<FlowGraph> graph, BaseFlowGraphHelper baseFlowGraphHelper) {
    this.flowGraph = graph;
    this.baseFlowGraphHelper = baseFlowGraphHelper;
  }

  /**
   * Add an element (i.e., a {@link DataNode}, or a {@link FlowEdge} to
   * the {@link FlowGraph} for an added, updated or modified node or edge file.
   * @param change
   */
  @Override
  public void addChange(DiffEntry change) {
    Path path = new Path(change.getNewPath());
    if (path.depth() == BaseFlowGraphHelper.NODE_FILE_DEPTH) {
      this.baseFlowGraphHelper.addDataNode(this.flowGraph.get(), change.getNewPath());
    } else if (path.depth() == BaseFlowGraphHelper.EDGE_FILE_DEPTH) {
      this.baseFlowGraphHelper.addFlowEdge(this.flowGraph.get(), change.getNewPath());
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
    if (path.depth() == BaseFlowGraphHelper.NODE_FILE_DEPTH) {
      this.baseFlowGraphHelper.removeDataNode(this.flowGraph.get(), change.getOldPath());
    } else if (path.depth() == BaseFlowGraphHelper.EDGE_FILE_DEPTH) {
      this.baseFlowGraphHelper.removeFlowEdge(this.flowGraph.get(), change.getOldPath());
    }
  }
}
