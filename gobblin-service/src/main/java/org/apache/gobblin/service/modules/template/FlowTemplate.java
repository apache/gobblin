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
import java.util.Map;

import com.typesafe.config.Config;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.Dag;

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
   * @return the {@link Dag<JobTemplate>} that backs the {@link FlowTemplate}.
   */
  Dag<JobTemplate> getDag() throws IOException;

  /**
   *
   * @return all configuration inside pre-written template.
   */
  Config getRawTemplateConfig();

  /**
   * @return list of input/output {@link DatasetDescriptor}s for the {@link FlowTemplate}.
   */
  List<Pair<DatasetDescriptor, DatasetDescriptor>> getInputOutputDatasetDescriptors();
}
