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

package org.apache.gobblin.service.modules.template_catalog;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.filesystem.PathAlterationListener;
import org.apache.gobblin.util.filesystem.PathAlterationListenerAdaptor;


/**
 * {@link FSFlowTemplateCatalog} that keeps a cache of flow and job templates. It has a
 * {@link org.apache.gobblin.util.filesystem.PathAlterationListener} on the root path, and clears the cache when a change
 * is detected so that the next call to {@link #getFlowTemplate(URI)} will use the updated files.
 */
@Slf4j
public class ObservingFSFlowEdgeTemplateCatalog extends UpdatableFSFlowTemplateCatalog {

  private AtomicBoolean shouldRefreshFlowGraph = new AtomicBoolean(false);

  public ObservingFSFlowEdgeTemplateCatalog(Config sysConfig, ReadWriteLock rwLock) throws IOException {
    super(sysConfig, rwLock);
  }

  @Override
  protected PathAlterationListener getListener() {
    return new FlowCatalogPathAlterationListener();
  }

  @Override
  protected void startUp() throws IOException {
    if (this.pathAlterationDetector != null) {
      this.pathAlterationDetector.start();
    }
  }

  @Override
  public boolean getAndSetShouldRefreshFlowGraph(boolean value) {
    return this.shouldRefreshFlowGraph.getAndSet(value);
  }

  /**
   * {@link org.apache.gobblin.util.filesystem.PathAlterationListener} that clears flow/job template cache if a file is
   * created or updated.
   */
  private class FlowCatalogPathAlterationListener extends PathAlterationListenerAdaptor {
    @Override
    public void onCheckDetectedChange() {
      clearTemplates();
      getAndSetShouldRefreshFlowGraph(true);
    }
  }
}
