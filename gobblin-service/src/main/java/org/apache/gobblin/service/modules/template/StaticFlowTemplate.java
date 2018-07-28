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
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.template_catalog.FlowCatalogWithTemplates;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.fs.Path;

import lombok.Getter;


/**
 * A {@link FlowTemplate} using a static {@link Config} as the raw configuration for the template.
 */
@Alpha
public class StaticFlowTemplate implements FlowTemplate {
  private static final long serialVersionUID = 84641624233978L;

  public static final String INPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.dataset.descriptor.input";
  public static final String OUTPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.dataset.descriptor.output";
  public static final String DATASET_DESCRIPTOR_CLASS_KEY = "class";

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
  private boolean isTemplateMaterialized;

  public StaticFlowTemplate(URI uri, String version, String description, Config config,
      FlowCatalogWithTemplates catalog)
      throws IOException, ReflectiveOperationException, SpecNotFoundException, JobTemplate.TemplateException {
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.inputOutputDatasetDescriptors = buildInputOutputDescriptors(config);
    this.rawConfig = config;
    this.catalog = catalog;
    URI flowTemplateDir = new Path(this.uri).getParent().toUri();
    this.jobTemplates = this.catalog.getJobTemplatesForFlow(flowTemplateDir);
  }

  //Constructor for testing purposes
  public StaticFlowTemplate(URI uri, String version, String description, Config config,
      FlowCatalogWithTemplates catalog, List<Pair<DatasetDescriptor, DatasetDescriptor>> inputOutputDatasetDescriptors, List<JobTemplate> jobTemplates)
      throws IOException, ReflectiveOperationException, SpecNotFoundException, JobTemplate.TemplateException {
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
   */
  private List<Pair<DatasetDescriptor, DatasetDescriptor>> buildInputOutputDescriptors(Config config)
      throws IOException, ReflectiveOperationException {
    if (!config.hasPath(INPUT_DATASET_DESCRIPTOR_PREFIX) || !config.hasPath(OUTPUT_DATASET_DESCRIPTOR_PREFIX)) {
      throw new IOException("Flow template must specify at least one input/output dataset descriptor");
    }
    int i = 0;
    String inputPrefix = Joiner.on(".").join(INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
    List<Pair<DatasetDescriptor, DatasetDescriptor>> result = Lists.newArrayList();
    while (config.hasPath(inputPrefix)) {
      Config inputDescriptorConfig = config.getConfig(inputPrefix);
      DatasetDescriptor inputDescriptor = getDatasetDescriptor(inputDescriptorConfig);
      String outputPrefix = Joiner.on(".").join(OUTPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i++));
      Config outputDescriptorConfig = config.getConfig(outputPrefix);
      DatasetDescriptor outputDescriptor = getDatasetDescriptor(outputDescriptorConfig);
      result.add(ImmutablePair.of(inputDescriptor, outputDescriptor));
      inputPrefix = Joiner.on(".").join(INPUT_DATASET_DESCRIPTOR_PREFIX, Integer.toString(i));
    }
    return result;
  }

  private DatasetDescriptor getDatasetDescriptor(Config descriptorConfig)
      throws ReflectiveOperationException {
    Class datasetDescriptorClass = Class.forName(descriptorConfig.getString(DATASET_DESCRIPTOR_CLASS_KEY));
    return (DatasetDescriptor) GobblinConstructorUtils
        .invokeLongestConstructor(datasetDescriptorClass, descriptorConfig);
  }

  @Override
  public Config getRawTemplateConfig() {
    return this.rawConfig;
  }

  private void ensureTemplateMaterialized()
      throws IOException {
    try {
      if (!isTemplateMaterialized) {
        this.dag = JobTemplateDagFactory.createDagFromJobTemplates(this.jobTemplates);
      }
      this.isTemplateMaterialized = true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<JobTemplate> getJobTemplates() {
    return this.jobTemplates;
  }

  @Override
  public Dag<JobTemplate> getDag()
      throws IOException {
    ensureTemplateMaterialized();
    return this.dag;
  }
}
