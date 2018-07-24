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

package org.apache.gobblin.service.modules.template;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigResolveOptions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;

/**
 * An interface primarily for representing a flow of {@link JobTemplate}s. It also has
 * method for retrieving required configs for every {@link JobTemplate} in the flow.
 */
@Alpha
public interface FlowTemplate extends Spec {

  /**
   * @return the {@link Collection} of {@link JobTemplate}s that belong to this {@link FlowTemplate}.
   */
  List<JobTemplate> getJobTemplates();

  /**
   *
   * @return all configuration inside pre-written template.
   */
  Config getRawTemplateConfig();

  /**
   * Checks if the {@link FlowTemplate} is resolvable using the provided {@link Config} object. A {@link FlowTemplate}
   * is resolvable only if each of the {@link JobTemplate}s in the flow is resolvable
   * @param userConfig User supplied Config
   * @return true if the {@link FlowTemplate} is resolvable
   */
  boolean isResolvable(Config userConfig) throws SpecNotFoundException, JobTemplate.TemplateException;

  /**
   * @param userConfig a list of user customized attributes.
   * @return list of input/output {@link DatasetDescriptor}s for the {@link FlowTemplate}.
   */
  List<Pair<DatasetDescriptor, DatasetDescriptor>> getInputOutputDatasetDescriptors(Config userConfig)
      throws IOException, ReflectiveOperationException;
}
