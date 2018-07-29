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
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.template_catalog.FlowCatalogWithTemplates;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link FlowTemplate} using a static {@link Config} as the raw configuration for the template.
 */
@Alpha
@Slf4j
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
  private List<JobTemplate> jobTemplates;

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
  public StaticFlowTemplate(URI uri, String version, String description, Config config, FlowCatalogWithTemplates catalog, List<JobTemplate> jobTemplates) {
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.rawConfig = config;
    this.catalog = catalog;
    this.jobTemplates = jobTemplates;
  }


  /**
   * Generate the input/output dataset descriptors for the {@link FlowTemplate}.
   * @param userConfig
   * @return a List of Input/Output DatasetDescriptors that resolve this {@link FlowTemplate}.
   */
  @Override
  public List<Pair<DatasetDescriptor, DatasetDescriptor>> getResolvingDatasetDescriptors(Config userConfig)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException {
    Config config = this.getResolvedFlowConfig(userConfig).resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));

    if (!config.hasPath(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX)
        || !config.hasPath(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX)) {
      throw new IOException("Flow template must specify at least one input/output dataset descriptor");
    }

    int i = 0;
    String inputPrefix = Joiner.on(".").join(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
    List<Pair<DatasetDescriptor, DatasetDescriptor>> result = Lists.newArrayList();
    while (config.hasPath(inputPrefix)) {
      try {
        Config inputDescriptorConfig = config.getConfig(inputPrefix);
        DatasetDescriptor inputDescriptor = getDatasetDescriptor(inputDescriptorConfig);
        String outputPrefix = Joiner.on(".")
            .join(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
        Config outputDescriptorConfig = config.getConfig(outputPrefix);
        DatasetDescriptor outputDescriptor = getDatasetDescriptor(outputDescriptorConfig);

        if (isResolvable(userConfig, inputDescriptor, outputDescriptor)) {
          result.add(ImmutablePair.of(inputDescriptor, outputDescriptor));
        }
      } catch (ReflectiveOperationException e) {
        //Cannot instantiate I/O dataset descriptor due to missing config; skip and try the next one.
      }
      inputPrefix = Joiner.on(".").join(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(++i));
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

  /**
   * Checks if the {@link FlowTemplate} is resolvable using the provided {@link Config} object. A {@link FlowTemplate}
   * is resolvable only if each of the {@link JobTemplate}s in the flow is resolvable
   * @param userConfig User supplied Config
   * @return true if the {@link FlowTemplate} is resolvable
   */
  @Override
  public boolean isResolvable(Config userConfig, DatasetDescriptor inputDescriptor, DatasetDescriptor outputDescriptor)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    Config inputDescriptorConfig = inputDescriptor.getRawConfig().atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX);
    Config outputDescriptorConfig = outputDescriptor.getRawConfig().atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX);
    userConfig = userConfig.withFallback(inputDescriptorConfig).withFallback(outputDescriptorConfig);

    ConfigResolveOptions resolveOptions = ConfigResolveOptions.defaults().setAllowUnresolved(true);

    for (JobTemplate template: this.jobTemplates) {
      Config templateConfig = template.getResolvedConfig(userConfig).resolve(resolveOptions);
      if (!template.getResolvedConfig(userConfig).resolve(resolveOptions).isResolved()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<Config> getResolvedJobConfigs(Config userConfig, DatasetDescriptor inputDescriptor,
      DatasetDescriptor outputDescriptor)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    Config inputDescriptorConfig = inputDescriptor.getRawConfig().atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX);
    Config outputDescriptorConfig = outputDescriptor.getRawConfig().atPath(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX);
    userConfig = userConfig.withFallback(inputDescriptorConfig).withFallback(outputDescriptorConfig);

    List<Config> resolvedJobConfigs = new ArrayList<>();
    for (JobTemplate jobTemplate: getJobTemplates()) {
      Config resolvedJobConfig = jobTemplate.getResolvedConfig(userConfig).resolve().withValue(
          ConfigurationKeys.JOB_TEMPLATE_PATH, ConfigValueFactory.fromAnyRef(jobTemplate.getUri().toString()));;
      resolvedJobConfigs.add(resolvedJobConfig);
    }
    return resolvedJobConfigs;
  }
}
