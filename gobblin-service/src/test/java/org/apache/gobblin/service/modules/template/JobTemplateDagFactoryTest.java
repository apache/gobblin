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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class JobTemplateDagFactoryTest {
  private static final String TEST_TEMPLATE_NAME = "test-template";
  private static final String TEST_FLOW_CONF_FILE_NAME="flow.conf";
  private static final String TEST_TEMPLATE_URI = "FS:///" + TEST_TEMPLATE_NAME + "/" + TEST_FLOW_CONF_FILE_NAME;
  FSFlowCatalog catalog;

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, SpecNotFoundException, JobTemplate.TemplateException {
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();

    // Create a FSFlowCatalog instance
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    this.catalog = new FSFlowCatalog(templateCatalogCfg);
  }

  @Test
  public void testCreateDagFromJobTemplates() throws Exception {
    FlowTemplate flowTemplate = catalog.getFlowTemplate(new URI(TEST_TEMPLATE_URI));
    List<JobTemplate> jobTemplates = flowTemplate.getJobTemplates();

    //Create a DAG from job templates.
    Dag<JobTemplate> jobTemplateDag = JobTemplateDagFactory.createDagFromJobTemplates(jobTemplates);

    //Test DAG properties
    Assert.assertEquals(jobTemplateDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobTemplateDag.getEndNodes().size(), 1);
    Assert.assertEquals(jobTemplateDag.getNodes().size(), 4);
    String startNodeName = new Path(jobTemplateDag.getStartNodes().get(0).getValue().getUri()).getName();
    Assert.assertEquals(startNodeName, "job1.conf");
    String endNodeName = new Path(jobTemplateDag.getEndNodes().get(0).getValue().getUri()).getName();
    Assert.assertEquals(endNodeName, "job4.conf");

    Dag.DagNode<JobTemplate> startNode = jobTemplateDag.getStartNodes().get(0);
    List<Dag.DagNode<JobTemplate>> nextNodes = jobTemplateDag.getChildren(startNode);
    Set<String> nodeSet = new HashSet<>();
    for(Dag.DagNode<JobTemplate> node: nextNodes) {
      nodeSet.add(new Path(node.getValue().getUri()).getName());
      Dag.DagNode<JobTemplate> nextNode = jobTemplateDag.getChildren(node).get(0);
      Assert.assertEquals(new Path(nextNode.getValue().getUri()).getName(), "job4.conf");
    }
    Assert.assertTrue(nodeSet.contains("job2.conf"));
    Assert.assertTrue(nodeSet.contains("job3.conf"));
  }
}