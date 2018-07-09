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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigResolveOptions;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.template_catalog.FlowCatalogWithTemplates;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link FlowTemplate} using a static {@link Config} as the raw configuration for the template.
 */
@Alpha
public class StaticFlowTemplate implements FlowTemplate {
  private static final long serialVersionUID = 84641624233978L;

  @Getter
  private URI uri;
  @Getter
  private String version;
  @Getter
  private String description;
  @Getter
  private transient FlowCatalogWithTemplates catalog;
  @Getter
  private List<Pair<DatasetDescriptor, DatasetDescriptor>> inputOutputDatasetDescriptors;
  @Getter
  private List<JobTemplate> jobTemplates;

  private transient Dag<JobTemplate> dag;

  private transient Config rawConfig;

  public StaticFlowTemplate(URI flowTemplateDirUri, String version, String description, Config config,
      FlowCatalogWithTemplates catalog)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    this.uri = flowTemplateDirUri;
    this.version = version;
    this.description = description;
    this.rawConfig = config;
    this.catalog = catalog;
    this.jobTemplates = this.catalog.getJobTemplatesForFlow(flowTemplateDirUri);
  }

  //Constructor for testing purposes
  public StaticFlowTemplate(URI uri, String version, String description, Config config,
      FlowCatalogWithTemplates catalog, List<Pair<DatasetDescriptor, DatasetDescriptor>> inputOutputDatasetDescriptors, List<JobTemplate> jobTemplates) {
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.inputOutputDatasetDescriptors = inputOutputDatasetDescriptors;
    this.rawConfig = config;
    this.catalog = catalog;
    this.jobTemplates = jobTemplates;
  }

  /**
   * Generate the input/output dataset descriptors for the {@link FlowTemplate}.
   * @param userConfig
   * @param options Config resolve options.
   * @return a List of Input/Output DatasetDescriptors associated with this {@link org.apache.gobblin.service.modules.flowgraph.FlowEdge}.
   */
  private List<Pair<DatasetDescriptor, DatasetDescriptor>> buildInputOutputDescriptors(Config userConfig, ConfigResolveOptions options)
      throws IOException, ReflectiveOperationException {
    Config config = this.getResolvedFlowConfig(userConfig).resolve(options);

    if (!config.hasPath(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX)
        || !config.hasPath(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX)) {
      throw new IOException("Flow template must specify at least one input/output dataset descriptor");
    }
    int i = 0;
    String inputPrefix = Joiner.on(".").join(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
    List<Pair<DatasetDescriptor, DatasetDescriptor>> result = Lists.newArrayList();
    while (config.hasPath(inputPrefix)) {
      Config inputDescriptorConfig = config.getConfig(inputPrefix);
      DatasetDescriptor inputDescriptor = getDatasetDescriptor(inputDescriptorConfig);
      String outputPrefix = Joiner.on(".").join(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i++));
      Config outputDescriptorConfig = config.getConfig(outputPrefix);
      DatasetDescriptor outputDescriptor = getDatasetDescriptor(outputDescriptorConfig);
      result.add(ImmutablePair.of(inputDescriptor, outputDescriptor));
      inputPrefix = Joiner.on(".").join(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
    }
    return result;
  }

  private DatasetDescriptor getDatasetDescriptor(Config descriptorConfig)
      throws ReflectiveOperationException {
    Class datasetDescriptorClass = Class.forName(descriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
    return (DatasetDescriptor) GobblinConstructorUtils.invokeLongestConstructor(datasetDescriptorClass, descriptorConfig);
  }

  @Override
  public Config getRawTemplateConfig() {
    return this.rawConfig;
  }

  @Override
  public List<JobTemplate> getJobTemplates() {
    return this.jobTemplates;
  }

  private Config getResolvedFlowConfig(Config userConfig) {
    return userConfig.withFallback(this.rawConfig);
  }

  @Override
  public List<Config> getResolvedJobTemplates(Config userConfig) throws SpecNotFoundException, JobTemplate.TemplateException {
    List<Config> resolvedJobTemplates = new ArrayList<>();
    for (JobTemplate jobTemplate: this.jobTemplates) {
      resolvedJobTemplates.add(userConfig.withFallback(jobTemplate.getRawTemplateConfig()));
    }
    return resolvedJobTemplates;
  }

  /**
   * Checks if the {@link FlowTemplate} is resolvable using the provided {@link Config} object. A {@link FlowTemplate}
   * is resolvable only if each of the {@link JobTemplate}s in the flow is resolvable
   * @param userConfig User supplied Config
   * @return true if the {@link FlowTemplate} is resolvable
   */
  @Override
  public boolean isResolvable(Config userConfig) throws SpecNotFoundException, JobTemplate.TemplateException {
    ConfigResolveOptions resolveOptions = ConfigResolveOptions.defaults().setAllowUnresolved(true);
    if (!userConfig.withFallback(this.rawConfig).resolve(resolveOptions).isResolved()) {
      return false;
    }

    for (JobTemplate template: this.jobTemplates) {
      Config tmpConfig = template.getResolvedConfig(userConfig).resolve(resolveOptions);
      if (!template.getResolvedConfig(userConfig).resolve(resolveOptions).isResolved()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param userConfig a list of user customized attributes.
   * @return list of input/output {@link DatasetDescriptor}s for the {@link FlowTemplate}.
   */
  @Override
  public List<Pair<DatasetDescriptor, DatasetDescriptor>> getInputOutputDatasetDescriptors(Config userConfig, ConfigResolveOptions options)
      throws IOException, ReflectiveOperationException {
      if (this.inputOutputDatasetDescriptors == null) {
        this.inputOutputDatasetDescriptors = buildInputOutputDescriptors(userConfig, options);
      }
      return this.inputOutputDatasetDescriptors;
  }

  /**
   * @param userConfig a list of user customized attributes.
   * @return list of input/output {@link DatasetDescriptor}s for the {@link FlowTemplate}.
   */
  @Override
  public List<Pair<DatasetDescriptor, DatasetDescriptor>> getInputOutputDatasetDescriptors(Config userConfig)
      throws IOException, ReflectiveOperationException {
    return getInputOutputDatasetDescriptors(userConfig, ConfigResolveOptions.defaults());
  }
}
