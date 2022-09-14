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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.filesystem.PathAlterationListener;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;


/**
 * An implementation of {@link PathAlterationListener} to listen for changes in a directory and apply it to a GaaS FlowGraph
 * Is invoked by {@link PathAlterationObserver} which would check a folder and perform recursive comparisons on files compared to
 * their last polled state. On any detected differences in files when a check is done, the {@link FlowGraph} will be updated.
 *
 * Unlike the {@link GitFlowGraphListener}, this class will reload the entire flowgraph on any detected change, instead loading only the diffs.
 */
@Slf4j
public class FSPathAlterationFlowGraphListener extends BaseFlowGraphListener implements PathAlterationListener {

  private final File graphDir;
  CountDownLatch initComplete;
  AtomicReference<FlowGraph> flowGraph;

  public FSPathAlterationFlowGraphListener(Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      AtomicReference<FlowGraph> graph, Map<URI, TopologySpec> topologySpecMap, String baseDirectory,
      String flowGraphFolderName, String javaPropsExtentions, String hoconFileExtensions, CountDownLatch initComplete) {
    super(flowTemplateCatalog, topologySpecMap, baseDirectory, flowGraphFolderName, javaPropsExtentions,
        hoconFileExtensions);
    this.graphDir = new File(baseDirectory);
    this.initComplete = initComplete;
    // Populate the flowgraph with any existing files
    if (!this.graphDir.exists()) {
      throw new RuntimeException(String.format("Flowgraph directory at path %s does not exist!", graphDir));
    }
    this.flowGraph = graph;
    this.populateFlowGraphAtomically();
  }

  @Override
  public void onStart(final PathAlterationObserver observer) {
  }

  @Override
  public void onFileCreate(final Path path) {
  }

  @Override
  public void onFileChange(final Path path) {
  }

  @Override
  public void onStop(final PathAlterationObserver observer) {
  }

  @Override
  public void onDirectoryCreate(final Path directory) {
  }

  @Override
  public void onDirectoryChange(final Path directory) {
  }

  @Override
  public void onDirectoryDelete(final Path directory) {
  }

  @Override
  public void onFileDelete(final Path path) {
  }

  @Override
  public void onCheckDetectedChange() {
    log.info("Detecting change in flowgraph files, reloading flowgraph");
    this.populateFlowGraphAtomically();
  }

  /**
   * Loads the entire flowgraph from the path configured in {@link org.apache.gobblin.configuration.ConfigurationKeys.FLOWGRAPH_BASE_DIR }
   * Expects nodes to be in the format of /flowGraphName/nodeA/nodeA.properties
   * Expects edges to be in the format of /flowGraphName/nodeA/nodeB/edgeAB.properties
   * The current flowgraph will be swapped atomically with the new flowgraph that is loaded
   */
  private void populateFlowGraphAtomically() {
    FlowGraph newFlowGraph = new BaseFlowGraph();
    try {
      List<Path> edges = new ArrayList<>();
      // All nodes must be added first before edges, otherwise edges may have a missing source or destination.
      // Need to convert files to Hadoop Paths to be compatible with FileAlterationListener
      Files.walk(this.graphDir.toPath()).forEach(fileName -> {
        if (!Files.isDirectory(fileName)) {
          if (checkFileLevelRelativeToRoot(new Path(fileName.toString()), NODE_FILE_DEPTH)) {
            addDataNode(newFlowGraph, fileName.toString());
          } else if (checkFileLevelRelativeToRoot(new Path(fileName.toString()), EDGE_FILE_DEPTH)) {
            edges.add(new Path(fileName.toString()));
          }
        }
      });
      for (Path edge : edges) {
        addFlowEdge(newFlowGraph, edge.toString());
      }
      this.flowGraph.set(newFlowGraph);
      // Reduce the countdown latch
      this.initComplete.countDown();
      log.info("Finished populating FSFlowgraph");
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Error while populating file based flowgraph at path %s", this.graphDir.toPath()), e);
    }
  }
}
