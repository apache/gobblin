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

package org.apache.gobblin.runtime.template;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.runtime.api.FlowTemplate;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.dag.Dag;
import org.apache.gobblin.runtime.dag.JobTemplateDagFactory;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;

import com.typesafe.config.Config;

import lombok.Getter;

public class StaticFlowTemplate implements FlowTemplate {
  @Getter
  private URI uri;
  @Getter
  private String version;
  @Getter
  private String description;
  @Getter
  private FlowCatalog catalog;

  private Dag<JobTemplate> dag;
  private Map<JobTemplate, Collection<String>> requiredConfigMap;

  private Config rawConfig;
  private boolean isTemplateMaterialized;

  public StaticFlowTemplate(URI uri, String version, String description, Config config, FlowCatalog catalog) {
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.rawConfig = config;
    this.catalog = catalog;
  }

  @Override
  public Config getRawTemplateConfig() {
    return this.rawConfig;
  }

  private void ensureTemplateMaterialized() throws IOException {
    try {
      if (!isTemplateMaterialized) {
        List<JobTemplate> jobTemplates = this.catalog.getJobTemplates(this.uri);
        this.dag = JobTemplateDagFactory.createDagFromJobTemplates(jobTemplates);
        this.requiredConfigMap = new HashMap<>();
        for (JobTemplate template : jobTemplates) {
          this.requiredConfigMap.put(template, template.getRequiredConfigList());
        }
      }
      this.isTemplateMaterialized = true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Dag<JobTemplate> getDag() throws IOException {
    ensureTemplateMaterialized();
    return this.dag;
  }

  @Override
  public Map<JobTemplate, Collection<String>> getRequiredConfigMap() throws IOException {
    ensureTemplateMaterialized();
    return this.requiredConfigMap;
  }
}
