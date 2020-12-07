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

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;


/**
 * A catalog that supports loading {@link FlowTemplate}s.
 */
public interface FlowCatalogWithTemplates {
  /**
   * Get {@link FlowTemplate} with given {@link URI}.
   * @throws SpecNotFoundException if a {@link JobTemplate} with given {@link URI} cannot be found.
   */
  FlowTemplate getFlowTemplate(URI uri)
      throws SpecNotFoundException, IOException, JobTemplate.TemplateException, URISyntaxException;

  /**
   *
   * @param flowTemplateDirURI URI of the flow template directory.
   * @return a list of {@link JobTemplate}s for a given flow identified by its {@link URI}.
   * @throws IOException
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   */
  public List<JobTemplate> getJobTemplatesForFlow(URI flowTemplateDirURI)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException;

}
