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
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.util.filesystem.PathAlterationListener;
import org.apache.gobblin.util.filesystem.PathAlterationListenerAdaptor;


/**
 * {@link FSFlowTemplateCatalog} that keeps a cache of flow and job templates. It has a
 * {@link org.apache.gobblin.util.filesystem.PathAlterationListener} on the root path, and clears the cache when a change
 * is detected so that the next call to {@link #getFlowTemplate(URI)} will use the updated files.
 */
@Slf4j
public class ObservingFSFlowEdgeTemplateCatalog extends FSFlowTemplateCatalog {
  private Map<URI, FlowTemplate> flowTemplateMap = new ConcurrentHashMap<>();
  private Map<URI, List<JobTemplate>> jobTemplateMap = new ConcurrentHashMap<>();
  private ReadWriteLock rwLock;

  private AtomicBoolean shouldRefreshFlowGraph = new AtomicBoolean(false);

  public ObservingFSFlowEdgeTemplateCatalog(Config sysConfig, ReadWriteLock rwLock) throws IOException {
    super(sysConfig);
    this.rwLock = rwLock;
  }

  @Override
  public FlowTemplate getFlowTemplate(URI flowTemplateDirURI)
      throws SpecNotFoundException, JobTemplate.TemplateException, IOException, URISyntaxException {
    FlowTemplate flowTemplate = flowTemplateMap.getOrDefault(flowTemplateDirURI, null);

    if (flowTemplate == null) {
      flowTemplate = super.getFlowTemplate(flowTemplateDirURI);
      flowTemplateMap.put(flowTemplateDirURI, flowTemplate);
    }

    return flowTemplate;
  }

  @Override
  public List<JobTemplate> getJobTemplatesForFlow(URI flowTemplateDirURI)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    List<JobTemplate> jobTemplates = jobTemplateMap.getOrDefault(flowTemplateDirURI, null);

    if (jobTemplates == null) {
      jobTemplates = super.getJobTemplatesForFlow(flowTemplateDirURI);
      jobTemplateMap.put(flowTemplateDirURI, jobTemplates);
    }

    return jobTemplates;
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
   * Clear cached templates so they will be reloaded next time {@link #getFlowTemplate(URI)} is called.
   * Also refresh git flow graph in case any edges that failed to be added on startup are successful now.
   */
  private void clearTemplates() {
    this.rwLock.writeLock().lock();
    log.info("Change detected, reloading flow templates.");
    flowTemplateMap.clear();
    jobTemplateMap.clear();
    getAndSetShouldRefreshFlowGraph(true);
    this.rwLock.writeLock().unlock();
  }

  /**
   * {@link org.apache.gobblin.util.filesystem.PathAlterationListener} that clears flow/job template cache if a file is
   * created or updated.
   */
  private class FlowCatalogPathAlterationListener extends PathAlterationListenerAdaptor {
    @Override
    public void onFileCreate(Path path) {
      clearTemplates();
    }

    @Override
    public void onFileChange(Path path) {
      clearTemplates();
    }
  }
}
