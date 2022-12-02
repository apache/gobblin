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
import java.util.concurrent.locks.ReadWriteLock;


import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;

public class UpdatableFSFlowTemplateCatalog extends FSFlowTemplateCatalog {
  private Map<URI, FlowTemplate> flowTemplateMap = new ConcurrentHashMap<>();
  private Map<URI, List<JobTemplate>> jobTemplateMap = new ConcurrentHashMap<>();
  private ReadWriteLock rwLock;

  public UpdatableFSFlowTemplateCatalog(Config sysConfig, ReadWriteLock rwLock) throws IOException {
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

  /**
   * Clear cached templates so they will be reloaded next time {@link #getFlowTemplate(URI)} is called.
   */
  public void clearTemplates() {
    this.rwLock.writeLock().lock();
    log.info("Change detected, reloading flow templates.");
    flowTemplateMap.clear();
    jobTemplateMap.clear();
    this.rwLock.writeLock().unlock();
  }
}