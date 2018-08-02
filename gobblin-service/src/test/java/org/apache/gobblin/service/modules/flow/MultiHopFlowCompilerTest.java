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

package org.apache.gobblin.service.modules.flow;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.spec_executorInstance.AbstractSpecExecutor;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowEdgeFactory;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Slf4j
public class MultiHopFlowCompilerTest {
  private FlowGraph flowGraph;
  private SpecCompiler specCompiler;

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, ReflectiveOperationException, FlowEdgeFactory.FlowEdgeCreationException {
    //Create a FlowGraph
    this.flowGraph = new BaseFlowGraph();

    //Add DataNodes to the graph from the node properties files
    URI dataNodesUri = MultiHopFlowCompilerTest.class.getClassLoader().getResource("flowgraph/datanodes").toURI();
    FileSystem fs = FileSystem.get(dataNodesUri, new Configuration());
    Path dataNodesPath = new Path(dataNodesUri);
    ConfigParseOptions options = ConfigParseOptions.defaults()
        .setSyntax(ConfigSyntax.PROPERTIES)
        .setAllowMissing(false);

    for (FileStatus fileStatus: fs.listStatus(dataNodesPath)) {
      try (InputStream is = fs.open(fileStatus.getPath())) {
        Config nodeConfig = ConfigFactory.parseReader(new InputStreamReader(is, Charsets.UTF_8), options);
        Class dataNodeClass = Class.forName(ConfigUtils
            .getString(nodeConfig, FlowGraphConfigurationKeys.DATA_NODE_CLASS, FlowGraphConfigurationKeys.DEFAULT_DATA_NODE_CLASS));
        DataNode dataNode = (DataNode) GobblinConstructorUtils.invokeLongestConstructor(dataNodeClass, nodeConfig);
        this.flowGraph.addDataNode(dataNode);
      }
    }

    //Create a FSFlowCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    // Create a FSFlowCatalog instance
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowCatalog flowCatalog = new FSFlowCatalog(templateCatalogCfg);


    //Add FlowEdges from the edge properties files
    URI flowEdgesURI = MultiHopFlowCompilerTest.class.getClassLoader().getResource("flowgraph/flowedges").toURI();
    fs = FileSystem.get(flowEdgesURI, new Configuration());
    Path flowEdgesPath = new Path(flowEdgesURI);
    for (FileStatus fileStatus: fs.listStatus(flowEdgesPath)) {
      log.warn(fileStatus.getPath().toString());
      try (InputStream is = fs.open(fileStatus.getPath())) {
        Config flowEdgeConfig = ConfigFactory.parseReader(new InputStreamReader(is, Charsets.UTF_8), options);
        Class flowEdgeFactoryClass = Class.forName(ConfigUtils.getString(flowEdgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_FACTORY_CLASS,
            FlowGraphConfigurationKeys.DEFAULT_FLOW_EDGE_FACTORY_CLASS));
        FlowEdgeFactory flowEdgeFactory = (FlowEdgeFactory) GobblinConstructorUtils.invokeLongestConstructor(flowEdgeFactoryClass, config);
        FlowEdge edge = flowEdgeFactory.createFlowEdge(flowEdgeConfig, flowCatalog);
        this.flowGraph.addFlowEdge(edge);
      }
    }
    this.specCompiler = new MultiHopFlowCompiler(config, this.flowGraph);
  }

  private FlowSpec createFlowSpec(String flowConfigResource, String source, String destination) throws IOException, URISyntaxException {
    //Create a flow spec
    Properties flowProperties = new Properties();
    flowProperties.put(ConfigurationKeys.JOB_SCHEDULE_KEY, "* * * * *");
    flowProperties.put(ConfigurationKeys.FLOW_GROUP_KEY, "testFlowGroup");
    flowProperties.put(ConfigurationKeys.FLOW_NAME_KEY, "testFlowName");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, source);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, destination);
    Config flowConfig = ConfigUtils.propertiesToConfig(flowProperties);

    //Get the input/output dataset config from a file
    URI flowConfigUri = MultiHopFlowCompilerTest.class.getClassLoader().getResource(flowConfigResource).toURI();
    Path flowConfigPath = new Path(flowConfigUri);
    FileSystem fs1 = FileSystem.get(flowConfigUri, new Configuration());
    try (InputStream is = fs1.open(flowConfigPath)) {
      Config datasetConfig = ConfigFactory.parseReader(new InputStreamReader(is, Charset.defaultCharset()));
      flowConfig = flowConfig.withFallback(datasetConfig).resolve();
    }

    FlowSpec.Builder flowSpecBuilder = null;
    flowSpecBuilder = FlowSpec.builder(new Path("/tmp/flowSpecCatalog").toUri())
        .withConfig(flowConfig)
        .withDescription("dummy description")
        .withVersion(FlowSpec.Builder.DEFAULT_VERSION);

    FlowSpec spec = flowSpecBuilder.build();
    return spec;
  }
  @Test
  public void testCompileFlow() throws URISyntaxException, IOException {
    FlowSpec spec = createFlowSpec("flow/flow.conf", "LocalFS-1", "ADLS-1");
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);
    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobDag.getEndNodes().size(), 1);

    //Get the 1st hop - Distcp from "LocalFS-1" to "HDFS-1"
    Dag.DagNode<JobExecutionPlan> startNode = jobDag.getStartNodes().get(0);
    JobExecutionPlan jobSpecWithExecutor = startNode.getValue();
    JobSpec jobSpec = jobSpecWithExecutor.getJobSpec();

    //Ensure the resolved job config for the first hop has the correct substitutions.
    Config jobConfig = jobSpec.getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-HDFS");
    String from = jobConfig.getString("from");
    String to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/out/testTeam/testDataset");
    Assert.assertEquals(to, "/data/out/testTeam/testDataset");
    String sourceFsUri = jobConfig.getString("fs.uri");
    Assert.assertEquals(sourceFsUri, "file:///");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), sourceFsUri);
    Assert.assertEquals(jobConfig.getString("state.store.fs.uri"), sourceFsUri);
    String targetFsUri = jobConfig.getString("target.filebased.fs.uri");
    Assert.assertEquals(targetFsUri, "hdfs://hadoopnn01.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("writer.fs.uri"), targetFsUri);
    Assert.assertEquals(jobConfig.getString("gobblin.dataset.pattern"), from);
    Assert.assertEquals(jobConfig.getString("data.publisher.final.dir"), to);
    Assert.assertEquals(jobConfig.getString("type"), "java");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.runtime.local.LocalJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "LOCAL");
    //Ensure the spec executor has the correct configurations
    SpecExecutor specExecutor = jobSpecWithExecutor.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "fs:///");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor");

    //Get the 2nd hop - "HDFS-1 to HDFS-1 : convert avro to json and encrypt"
    Assert.assertEquals(jobDag.getChildren(startNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> secondHopNode = jobDag.getChildren(startNode).get(0);
    jobSpecWithExecutor = secondHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:convert-to-json-and-encrypt");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/out/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.data.directory"), from);
    Assert.assertEquals(jobConfig.getString("data.publisher.final.dir"), to);
    specExecutor = jobSpecWithExecutor.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban01.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Get the 3rd hop - "Distcp HDFS-1 to HDFS-3"
    Assert.assertEquals(jobDag.getChildren(secondHopNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> thirdHopNode = jobDag.getChildren(secondHopNode).get(0);
    jobSpecWithExecutor = thirdHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-HDFS");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "hdfs://hadoopnn01.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "hdfs://hadoopnn03.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("type"), "hadoopJava");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.azkaban.AzkabanJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "MAPREDUCE");
    //Ensure the spec executor has the correct configurations
    specExecutor = jobSpecWithExecutor.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban01.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Get the 4th hop - "Distcp from HDFS3 to ADLS-1"
    Assert.assertEquals(jobDag.getChildren(thirdHopNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> fourthHopNode = jobDag.getChildren(thirdHopNode).get(0);
    jobSpecWithExecutor = fourthHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-ADL");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "hdfs://hadoopnn03.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "adl://azuredatalakestore.net/");
    Assert.assertEquals(jobConfig.getString("type"), "hadoopJava");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.azkaban.AzkabanJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "MAPREDUCE");
    Assert.assertEquals(jobConfig.getString("dfs.adls.oauth2.client.id"), "1234");
    Assert.assertEquals(jobConfig.getString("writer.encrypted.dfs.adls.oauth2.credential"), "credential");
    Assert.assertEquals(jobConfig.getString("encrypt.key.loc"), "/user/testUser/master.password");
    //Ensure the spec executor has the correct configurations
    specExecutor = jobSpecWithExecutor.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban03.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Ensure the fourth hop is the last
    Assert.assertEquals(jobDag.getEndNodes().get(0), fourthHopNode);
  }

  @Test (dependsOnMethods = "testCompileFlow")
  public void testCompileFlowAfterFirstEdgeDeletion() throws URISyntaxException, IOException {
    //Delete the self edge on HDFS-1 that performs convert-to-json-and-encrypt.
    this.flowGraph.deleteFlowEdge("HDFS-1:HDFS-1:hdfsConvertToJsonAndEncrypt");

    FlowSpec spec = createFlowSpec("flow/flow.conf", "LocalFS-1", "ADLS-1");
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobDag.getEndNodes().size(), 1);

    //Get the 1st hop - Distcp from "LocalFS-1" to "HDFS-2"
    Dag.DagNode<JobExecutionPlan> startNode = jobDag.getStartNodes().get(0);
    JobExecutionPlan jobExecutionPlan = startNode.getValue();
    JobSpec jobSpec = jobExecutionPlan.getJobSpec();

    //Ensure the resolved job config for the first hop has the correct substitutions.
    Config jobConfig = jobSpec.getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-HDFS");
    String from = jobConfig.getString("from");
    String to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/out/testTeam/testDataset");
    Assert.assertEquals(to, "/data/out/testTeam/testDataset");
    String sourceFsUri = jobConfig.getString("fs.uri");
    Assert.assertEquals(sourceFsUri, "file:///");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), sourceFsUri);
    Assert.assertEquals(jobConfig.getString("state.store.fs.uri"), sourceFsUri);
    String targetFsUri = jobConfig.getString("target.filebased.fs.uri");
    Assert.assertEquals(targetFsUri, "hdfs://hadoopnn02.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("writer.fs.uri"), targetFsUri);
    Assert.assertEquals(jobConfig.getString("gobblin.dataset.pattern"), from);
    Assert.assertEquals(jobConfig.getString("data.publisher.final.dir"), to);
    Assert.assertEquals(jobConfig.getString("type"), "java");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.runtime.local.LocalJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "LOCAL");
    //Ensure the spec executor has the correct configurations
    SpecExecutor specExecutor = jobExecutionPlan.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "fs:///");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor");

    //Get the 2nd hop - "HDFS-2 to HDFS-2 : convert avro to json and encrypt"
    Assert.assertEquals(jobDag.getChildren(startNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> secondHopNode = jobDag.getChildren(startNode).get(0);
    jobExecutionPlan = secondHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:convert-to-json-and-encrypt");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/out/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.data.directory"), from);
    Assert.assertEquals(jobConfig.getString("data.publisher.final.dir"), to);
    specExecutor = jobExecutionPlan.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban02.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Get the 3rd hop - "Distcp HDFS-2 to HDFS-4"
    Assert.assertEquals(jobDag.getChildren(secondHopNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> thirdHopNode = jobDag.getChildren(secondHopNode).get(0);
    jobExecutionPlan = thirdHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-HDFS");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "hdfs://hadoopnn02.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "hdfs://hadoopnn04.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("type"), "hadoopJava");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.azkaban.AzkabanJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "MAPREDUCE");
    //Ensure the spec executor has the correct configurations
    specExecutor = jobExecutionPlan.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban02.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Get the 4th hop - "Distcp from HDFS4 to ADLS-1"
    Assert.assertEquals(jobDag.getChildren(thirdHopNode).size(), 1);
    Dag.DagNode<JobExecutionPlan> fourthHopNode = jobDag.getChildren(thirdHopNode).get(0);
    jobExecutionPlan = fourthHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();
    Assert.assertEquals(jobConfig.getString("job.name"), "testFlowGroup:testFlowName:Distcp-HDFS-ADL");
    from = jobConfig.getString("from");
    to = jobConfig.getString("to");
    Assert.assertEquals(from, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(to, "/data/encrypted/testTeam/testDataset");
    Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "hdfs://hadoopnn04.grid.linkedin.com:8888/");
    Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "adl://azuredatalakestore.net/");
    Assert.assertEquals(jobConfig.getString("type"), "hadoopJava");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.azkaban.AzkabanJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "MAPREDUCE");
    Assert.assertEquals(jobConfig.getString("dfs.adls.oauth2.client.id"), "1234");
    Assert.assertEquals(jobConfig.getString("writer.encrypted.dfs.adls.oauth2.credential"), "credential");
    Assert.assertEquals(jobConfig.getString("encrypt.key.loc"), "/user/testUser/master.password");
    //Ensure the spec executor has the correct configurations
    specExecutor = jobExecutionPlan.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "https://azkaban04.gobblin.net:8443");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest.TestAzkabanSpecExecutor");

    //Ensure the fourth hop is the last
    Assert.assertEquals(jobDag.getEndNodes().get(0), fourthHopNode);
  }

  @Test (dependsOnMethods = "testCompileFlowAfterFirstEdgeDeletion")
  public void testCompileFlowAfterSecondEdgeDeletion() throws URISyntaxException, IOException {
    //Delete the self edge on HDFS-2 that performs convert-to-json-and-encrypt.
    this.flowGraph.deleteFlowEdge("HDFS-2:HDFS-2:hdfsConvertToJsonAndEncrypt");

    FlowSpec spec = createFlowSpec("flow/flow.conf", "LocalFS-1", "ADLS-1");
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    //Ensure no path to destination.
    Assert.assertTrue(jobDag.isEmpty());
  }

  @Test (dependsOnMethods = "testCompileFlowAfterSecondEdgeDeletion")
  public void testMulticastPath() throws IOException, URISyntaxException {
    FlowSpec spec = createFlowSpec("flow/multicastFlow.conf", "LocalFS-1", "HDFS-3,HDFS-4");
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getEndNodes().size(), 2);
    Assert.assertEquals(jobDag.getStartNodes().size(), 2);

    int i = 1;
    //First hop must be from LocalFS to HDFS-1 and HDFS-2
    for (Dag.DagNode<JobExecutionPlan> dagNode : jobDag.getStartNodes()) {
      JobExecutionPlan jobExecutionPlan = dagNode.getValue();
      Config jobConfig = jobExecutionPlan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "file:///");
      Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "hdfs://hadoopnn0" + i++ + ".grid.linkedin.com:8888/");
    }

    i = 1;
    //Second hop must be from HDFS-1/HDFS-2 to HDFS-3/HDFS-4 respectively.
    for (Dag.DagNode<JobExecutionPlan> dagNode : jobDag.getStartNodes()) {
      List<Dag.DagNode<JobExecutionPlan>> nextNodes = jobDag.getChildren(dagNode);
      Assert.assertEquals(nextNodes.size(), 1);
      JobExecutionPlan jobExecutionPlan = nextNodes.get(0).getValue();
      Config jobConfig = jobExecutionPlan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString("source.filebased.fs.uri"), "hdfs://hadoopnn0" + i + ".grid.linkedin.com:8888/");
      Assert.assertEquals(jobConfig.getString("target.filebased.fs.uri"), "hdfs://hadoopnn0" + (i + 2) + ".grid.linkedin.com:8888/");
      i += 1;
    }
  }

  @AfterClass
  public void tearDown() {
  }

  public static class TestAzkabanSpecExecutor extends AbstractSpecExecutor {
    // Executor Instance
    protected final Config config;

    private SpecProducer<Spec> azkabanSpecProducer;

    public TestAzkabanSpecExecutor(Config config) {
      super(config);
      this.config = config;
    }

    @Override
    protected void startUp() throws Exception {
      //Do nothing
    }

    @Override
    protected void shutDown() throws Exception {
      //Do nothing
    }

    @Override
    public Future<String> getDescription() {
      return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + specExecutorInstanceUri, null);
    }

    @Override
    public Future<? extends SpecProducer> getProducer() {
      return new CompletedFuture<>(this.azkabanSpecProducer, null);
    }

    @Override
    public Future<Config> getConfig() {
      return new CompletedFuture<>(config, null);
    }

    @Override
    public Future<String> getHealth() {
      return new CompletedFuture<>("Healthy", null);
    }

  }
}