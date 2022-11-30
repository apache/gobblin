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

package org.apache.gobblin.service.modules.spec;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.ConfigUtils;


public class JobExecutionPlanDagFactoryTest {
  private static final String TEST_TEMPLATE_NAME = "flowEdgeTemplate";
  private static final String TEST_TEMPLATE_URI = "FS:///" + TEST_TEMPLATE_NAME;
  private SpecExecutor specExecutor;
  private List<JobTemplate> jobTemplates;

  @BeforeClass
  public void setUp() throws URISyntaxException, IOException, SpecNotFoundException, JobTemplate.TemplateException {
    // Create a FSFlowTemplateCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowTemplateCatalog catalog = new FSFlowTemplateCatalog(templateCatalogCfg);
    FlowTemplate flowTemplate = catalog.getFlowTemplate(new URI(TEST_TEMPLATE_URI));
    this.jobTemplates = flowTemplate.getJobTemplates();

    //Create a spec executor instance
    properties = new Properties();
    properties.put("specStore.fs.dir", "/tmp/testSpecStoreDir");
    properties.put("specExecInstance.capabilities", "source:destination");
    Config specExecutorConfig = ConfigUtils.propertiesToConfig(properties);
    this.specExecutor = new InMemorySpecExecutor(specExecutorConfig);
  }

  @Test
  public void testCreateDag() throws Exception {
    //Create a list of JobExecutionPlans
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    for (JobTemplate jobTemplate: this.jobTemplates) {
      Config config = jobTemplate.getRawTemplateConfig()
          .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef("testFlowName"))
          .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("testFlowGroup"))
          .withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));

      String jobSpecUri = Files.getNameWithoutExtension(new Path(jobTemplate.getUri()).getName());
      jobExecutionPlans.add(new JobExecutionPlan(JobSpec.builder(jobSpecUri).withConfig(config).
          withVersion("1").withTemplate(jobTemplate.getUri()).build(), specExecutor));
    }

    //Create a DAG from job execution plans.
    Dag<JobExecutionPlan> dag = new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);

    //Test DAG properties
    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getEndNodes().size(), 1);
    Assert.assertEquals(dag.getNodes().size(), 4);
    String startNodeName = new Path(dag.getStartNodes().get(0).getValue().getJobSpec().getUri()).getName();
    Assert.assertEquals(startNodeName, "job1");
    String templateUri = new Path(dag.getStartNodes().get(0).getValue().getJobSpec().getTemplateURI().get()).getName();
    Assert.assertEquals(templateUri, "job1.job");
    String endNodeName = new Path(dag.getEndNodes().get(0).getValue().getJobSpec().getUri()).getName();
    Assert.assertEquals(endNodeName, "job4");
    templateUri = new Path(dag.getEndNodes().get(0).getValue().getJobSpec().getTemplateURI().get()).getName();
    Assert.assertEquals(templateUri, "job4.job");

    Dag.DagNode<JobExecutionPlan> startNode = dag.getStartNodes().get(0);
    List<Dag.DagNode<JobExecutionPlan>> nextNodes = dag.getChildren(startNode);
    Set<String> nodeSet = new HashSet<>();
    for (Dag.DagNode<JobExecutionPlan> node: nextNodes) {
      nodeSet.add(new Path(node.getValue().getJobSpec().getUri()).getName());
      Dag.DagNode<JobExecutionPlan> nextNode = dag.getChildren(node).get(0);
      Assert.assertEquals(new Path(nextNode.getValue().getJobSpec().getUri()).getName(), "job4");
    }
    Assert.assertTrue(nodeSet.contains("job2"));
    Assert.assertTrue(nodeSet.contains("job3"));
  }

  @Test
  public void testCreateDagWithDuplicateJobNames() throws Exception {
    Config flowConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup").build();
    Config flowConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup").build();
    Config flowConfig3 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup").build();
    List<Config> flowConfigs = Arrays.asList(flowConfig1, flowConfig2, flowConfig3);

    Config jobConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job1")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName1").build();
    Config jobConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job2")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName2").build();
    Config jobConfig3 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job1")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName3").build();
    List<Config> jobConfigs = Arrays.asList(jobConfig1, jobConfig2, jobConfig3);

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      Config jobConfig = jobConfigs.get(i);
      if (i > 0) {
        String previousJobName = jobExecutionPlans.get(jobExecutionPlans.size() - 1).getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY);
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef(previousJobName));
      }

      FlowSpec flowSpec = FlowSpec.builder("testFlowSpec").withConfig(flowConfigs.get(i)).build();
      jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec, jobConfig.withValue(ConfigurationKeys.JOB_TEMPLATE_PATH,
          ConfigValueFactory.fromAnyRef("testUri")), new InMemorySpecExecutor(ConfigFactory.empty()), 0L, ConfigFactory.empty()));
    }

    Dag<JobExecutionPlan> dag = new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);

    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getEndNodes().size(), 1);
    Assert.assertEquals(dag.getNodes().size(), 3);
    Assert.assertNull(dag.getNodes().get(0).getParentNodes());
    Assert.assertEquals(dag.getNodes().get(1).getParentNodes().size(), 1);
    Assert.assertEquals(dag.getNodes().get(2).getParentNodes().size(), 1);
    Assert.assertEquals(dag.getNodes().get(1).getParentNodes().get(0), dag.getNodes().get(0));
    Assert.assertEquals(dag.getNodes().get(2).getParentNodes().get(0), dag.getNodes().get(1));
  }

  @Test
  public void testCreateDagAdhoc() throws Exception {
    Config flowConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup")
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/2 * * * * ?").build();
    Config flowConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup").build();

    List<Config> flowConfigs = Arrays.asList(flowConfig1, flowConfig2);

    Config jobConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job1")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName1")
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/2 * * * * ?").build();
    Config jobConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job2")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName2").build();
    List<Config> jobConfigs = Arrays.asList(jobConfig1, jobConfig2);

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      Config jobConfig = jobConfigs.get(i);
      FlowSpec flowSpec = FlowSpec.builder("testFlowSpec").withConfig(flowConfigs.get(i)).build();
      jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec, jobConfig.withValue(ConfigurationKeys.JOB_TEMPLATE_PATH,
          ConfigValueFactory.fromAnyRef("testUri")), new InMemorySpecExecutor(ConfigFactory.empty()), 0L, ConfigFactory.empty()));
    }

    Dag<JobExecutionPlan> dag1 = new JobExecutionPlanDagFactory().createDag(Arrays.asList(jobExecutionPlans.get(0)));
    Dag<JobExecutionPlan> dag2 = new JobExecutionPlanDagFactory().createDag(Arrays.asList(jobExecutionPlans.get(1)));


    Assert.assertEquals(dag1.getStartNodes().size(), 1);
    Assert.assertEquals(dag1.getEndNodes().size(), 1);
    Assert.assertEquals(dag1.getNodes().size(), 1);
    Assert.assertEquals(dag2.getStartNodes().size(), 1);
    Assert.assertEquals(dag2.getEndNodes().size(), 1);
    Assert.assertEquals(dag2.getNodes().size(), 1);
    // Dag1 is scheduled so should be adhoc and output metrics, but not dag2
    Assert.assertTrue(dag1.getStartNodes().get(0).getValue().getJobSpec().getConfig().getBoolean(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS));
    Assert.assertFalse(dag2.getStartNodes().get(0).getValue().getJobSpec().getConfig().getBoolean(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS));
  }

  @Test
  public void testCreateDagLongName() throws Exception {
    // flowName and flowGroup are both 128 characters long, the maximum for flowName and flowGroup
    Config flowConfig = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "uwXJwZPAPygvmSAfhtrzXL7ovIEKOBZdulBiNIGzaT7vILrK9QB5EDJj0fc4pkgNHuIKZ3d18TZzyH6a9HpaZACwpWpIpf8SYcSfKtXeoF8IJY064BqEUXR32k3ox31G")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "4mdfSGSv6GoFW7ICWubN2ORK4s5PMTQ60yIWkcbJOVneTSPn12cXT5ueEgij907tjzLlbcjdVjWFITFf9Y5sB9i0EvKGmTbUF98hJGoQlAhmottaipDEFTdbyzt5Loxg")
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/2 * * * * ?").build();

    Config jobConfig = ConfigBuilder.create()
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName1")
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/2 * * * * ?").build();

    FlowSpec flowSpec = FlowSpec.builder("testFlowSpec").withConfig(flowConfig).build();
    JobExecutionPlan jobExecutionPlan = new JobExecutionPlan.Factory().createPlan(flowSpec, jobConfig.withValue(ConfigurationKeys.JOB_TEMPLATE_PATH,
        ConfigValueFactory.fromAnyRef("testUri")), new InMemorySpecExecutor(ConfigFactory.empty()), 0L, ConfigFactory.empty());

    Dag<JobExecutionPlan> dag1 = new JobExecutionPlanDagFactory().createDag(Arrays.asList(jobExecutionPlan));

    Assert.assertEquals(dag1.getStartNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY).length(), 142);

  }

}