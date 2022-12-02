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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flow.MultiHopFlowCompiler;
import org.apache.gobblin.service.modules.template_catalog.UpdatableFSFlowTemplateCatalog;
import org.apache.gobblin.util.filesystem.PathAlterationListener;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;


/**
 * An implementation of {@link PathAlterationListener} to listen for changes in a directory and apply it to a GaaS FlowGraph
 * Is invoked by {@link PathAlterationObserver} which would check a folder and perform recursive comparisons on files compared to
 * their last polled state. On any detected differences in files when a check is done, the {@link FlowGraph} will be updated.
 *
 */
@Slf4j
public class FSPathAlterationFlowGraphListener implements PathAlterationListener {
  private final MultiHopFlowCompiler compiler;
  private final BaseFlowGraphHelper flowGraphHelper;
  private Optional<UpdatableFSFlowTemplateCatalog> flowTemplateCatalog;
  private final boolean shouldMonitorTemplateCatalog;

  public FSPathAlterationFlowGraphListener(Optional<UpdatableFSFlowTemplateCatalog> flowTemplateCatalog,
      MultiHopFlowCompiler compiler, String baseDirectory, BaseFlowGraphHelper flowGraphHelper, boolean shouldMonitorTemplateCatalog) {
    this.flowGraphHelper = flowGraphHelper;
    this.flowTemplateCatalog = flowTemplateCatalog;
    this.shouldMonitorTemplateCatalog = shouldMonitorTemplateCatalog;
    File graphDir = new File(baseDirectory);
    // Populate the flowgraph with any existing files
    if (!graphDir.exists()) {
      throw new RuntimeException(String.format("Flowgraph directory at path %s does not exist!", graphDir));
    }
    this.compiler = compiler;
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
    if (this.shouldMonitorTemplateCatalog) {
      // Clear template cache as templates are colocated with the flowgraph, and thus could have been updated too
      this.flowTemplateCatalog.get().clearTemplates();
    }
    FlowGraph newGraph = this.flowGraphHelper.generateFlowGraph();
    if (newGraph != null) {
      this.compiler.setFlowGraph(newGraph);
    }
  }
}
