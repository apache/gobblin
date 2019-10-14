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
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.testng.collections.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FSFlowTemplateCatalogTest {
  public static final String TEST_TEMPLATE_NAME = "flowEdgeTemplate";
  public static final String TEST_TEMPLATE_DIR_URI = "FS:///" + TEST_TEMPLATE_NAME;

  @Test
  public void testGetFlowTemplate() throws Exception {
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    // Create a FSFlowTemplateCatalog instance
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowTemplateCatalog catalog = new FSFlowTemplateCatalog(templateCatalogCfg);
    FlowTemplate flowTemplate = catalog.getFlowTemplate(new URI(TEST_TEMPLATE_DIR_URI));

    //Basic sanity check for the FlowTemplate

    List<JobTemplate> jobTemplates = flowTemplate.getJobTemplates();
    Assert.assertEquals(jobTemplates.size(), 4);
    for (int i = 0; i < 4; i++) {
      String uri = new Path(jobTemplates.get(i).getUri()).getName().split("\\.")[0];
      String templateId = uri.substring(uri.length() - 1);
      for (int j = 0; j < 2; j++) {
        Config jobTemplateConfig = jobTemplates.get(i).getRawTemplateConfig();
        String suffix = templateId + Integer.toString(j + 1);
        Assert.assertEquals(jobTemplateConfig.getString("key" + suffix), "val" + suffix);
      }
    }

    Config flowConfig = ConfigFactory.empty().withValue("team.name", ConfigValueFactory.fromAnyRef("test-team"))
        .withValue("dataset.name", ConfigValueFactory.fromAnyRef("test-dataset"));

    List<Pair<DatasetDescriptor, DatasetDescriptor>> inputOutputDescriptors = flowTemplate.getDatasetDescriptors(flowConfig, true);
    Assert.assertTrue(inputOutputDescriptors.size() == 2);
    List<String> dirs = Lists.newArrayList("inbound", "outbound");
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        FSDatasetDescriptor datasetDescriptor;
        if (j == 0) {
          datasetDescriptor = (FSDatasetDescriptor) inputOutputDescriptors.get(i).getLeft();
        } else {
          datasetDescriptor = (FSDatasetDescriptor) inputOutputDescriptors.get(i).getRight();
        }
        Assert.assertEquals(datasetDescriptor.getPlatform(), "hdfs");
        Assert.assertEquals(datasetDescriptor.getFormatConfig().getFormat(), "avro");
        Assert.assertEquals(datasetDescriptor.getPath(), "/data/" + dirs.get(i) + "/test-team/test-dataset");
      }
    }
    Config flowTemplateConfig = flowTemplate.getRawTemplateConfig();
    Assert.assertEquals(flowTemplateConfig.getString(DatasetDescriptorConfigKeys.FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX + ".0."
        + DatasetDescriptorConfigKeys.CLASS_KEY), FSDatasetDescriptor.class.getCanonicalName());
    Assert.assertEquals(flowTemplateConfig.getString(DatasetDescriptorConfigKeys.FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX
        + ".0." + DatasetDescriptorConfigKeys.CLASS_KEY), FSDatasetDescriptor.class.getCanonicalName());
  }
}