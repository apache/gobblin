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

import java.net.URI;
import java.util.List;
import java.util.Properties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.dataset.HdfsDatasetDescriptor;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.testng.collections.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FSFlowCatalogTest {
  private static final String TEST_TEMPLATE_NAME = "test-template";
  private static final String TEST_FLOW_CONF_FILE_NAME="flow.conf";
  private static final String TEST_TEMPLATE_URI = "FS:///" + TEST_TEMPLATE_NAME + "/" + TEST_FLOW_CONF_FILE_NAME;

  @Test
  public void testGetFlowTemplate() throws Exception {
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    // Create a FSFlowCatalog instance
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowCatalog catalog = new FSFlowCatalog(templateCatalogCfg);
    FlowTemplate flowTemplate = catalog.getFlowTemplate(new URI(TEST_TEMPLATE_URI));

    //Basic sanity check for the FlowTemplate
    Dag<JobTemplate> jobTemplateDag = flowTemplate.getDag();
    List<Dag.DagNode<JobTemplate>> dagNodes = jobTemplateDag.getNodes();
    Assert.assertTrue(dagNodes.size() == 4);
    Assert.assertEquals(jobTemplateDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobTemplateDag.getEndNodes().size(), 1);
    Dag.DagNode<JobTemplate> dagNode = jobTemplateDag.getStartNodes().get(0);
    URI startNodeUri = this.getClass().getClassLoader().getResource("template_catalog/test-template/jobs/job1.conf").toURI();
    URI endNodeUri = this.getClass().getClassLoader().getResource("template_catalog/test-template/jobs/job4.conf").toURI();
    Assert.assertEquals(jobTemplateDag.getStartNodes().get(0).getValue().getUri(), startNodeUri);
    Assert.assertEquals(jobTemplateDag.getEndNodes().get(0).getValue().getUri(), endNodeUri);

    List<JobTemplate> jobTemplates = flowTemplate.getJobTemplates();
    Assert.assertEquals(jobTemplates.size(), 4);
    for(int i=0; i<4; i++) {
      String uri = new Path(jobTemplates.get(i).getUri()).getName().split("\\.")[0];
      String templateId = uri.substring(uri.length() - 1);
      for(int j=0; j<2; j++) {
        Config jobTemplateConfig = jobTemplates.get(i).getRawTemplateConfig();
        String suffix = templateId + Integer.toString(j+1);
        Assert.assertEquals(jobTemplateConfig.getString("key" + suffix), "val" + suffix);
      }
    }

    List<Pair<DatasetDescriptor, DatasetDescriptor>> inputOutputDescriptors = flowTemplate.getInputOutputDatasetDescriptors();
    Assert.assertTrue(inputOutputDescriptors.size() == 2);
    List<String> dirs = Lists.newArrayList("inbound", "outbound");
    for(int i=0; i<2; i++) {
      for (int j=0; j<2; j++) {
        HdfsDatasetDescriptor datasetDescriptor;
        if (j == 0) {
          datasetDescriptor = (HdfsDatasetDescriptor) inputOutputDescriptors.get(i).getLeft();
        } else {
          datasetDescriptor = (HdfsDatasetDescriptor) inputOutputDescriptors.get(i).getRight();
        }
        Assert.assertEquals(datasetDescriptor.getPlatform(), "hdfs");
        Assert.assertEquals(datasetDescriptor.getType(),
            "org.apache.gobblin.service.modules.dataset.BaseHdfsDatasetDescriptor");
        Assert.assertEquals(datasetDescriptor.getFormat(), "avro");
        Assert.assertEquals(datasetDescriptor.getPath(), "/data/" + dirs.get(i) + "/<TEAM_NAME>/<DATASET_NAME>");
      }
    }
    Config flowTemplateConfig = flowTemplate.getRawTemplateConfig();
    Assert.assertEquals(flowTemplateConfig.getString("gobblin.flow.dataset.descriptor.input.0.class"), "org.apache.gobblin.service.modules.dataset.BaseHdfsDatasetDescriptor");
    Assert.assertEquals(flowTemplateConfig.getString("gobblin.flow.dataset.descriptor.output.0.class"), "org.apache.gobblin.service.modules.dataset.BaseHdfsDatasetDescriptor");
  }
}