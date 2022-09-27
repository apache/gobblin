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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_executorInstance.AbstractSpecExecutor;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowEdgeFactory;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Slf4j
public class MultiHopFlowCompilerTest {
  private AtomicReference<FlowGraph> flowGraph;
  private MultiHopFlowCompiler specCompiler;
  private final String TESTDIR = "/tmp/mhCompiler/gitFlowGraphTestDir";

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, ReflectiveOperationException, FlowEdgeFactory.FlowEdgeCreationException {
    //Create a FlowGraph
    this.flowGraph = new AtomicReference<>(new BaseFlowGraph());

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
        this.flowGraph.get().addDataNode(dataNode);
      }
    }

    URI specExecutorCatalogUri = this.getClass().getClassLoader().getResource("topologyspec_catalog").toURI();
    Map<URI, TopologySpec> topologySpecMap = buildTopologySpecMap(specExecutorCatalogUri);

    //Create a FSFlowTemplateCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowTemplateCatalog flowCatalog = new FSFlowTemplateCatalog(templateCatalogCfg);

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
        List<String> specExecutorNames = ConfigUtils.getStringList(flowEdgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY);
        List<SpecExecutor> specExecutors = new ArrayList<>();
        for (String specExecutorName: specExecutorNames) {
          specExecutors.add(topologySpecMap.get(new URI(specExecutorName)).getSpecExecutor());
        }
        FlowEdge edge = flowEdgeFactory.createFlowEdge(flowEdgeConfig, flowCatalog, specExecutors);
        this.flowGraph.get().addFlowEdge(edge);
      }
    }
    this.specCompiler = new MultiHopFlowCompiler(config, this.flowGraph);
  }

  /**
   * A helper method to return a {@link TopologySpec} map, given a {@link org.apache.gobblin.runtime.spec_catalog.TopologyCatalog}.
   * @param topologyCatalogUri pointing to the location of the {@link org.apache.gobblin.runtime.spec_catalog.TopologyCatalog}
   * @return a {@link TopologySpec} map.
   */
  public static Map<URI, TopologySpec> buildTopologySpecMap(URI topologyCatalogUri)
      throws IOException, URISyntaxException, ReflectiveOperationException {
    FileSystem fs = FileSystem.get(topologyCatalogUri, new Configuration());
    PathFilter extensionFilter = file -> {
      for (String extension : Lists.newArrayList(".properties")) {
        if (file.getName().endsWith(extension)) {
          return true;
        }
      }
      return false;
    };

    Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
    for (FileStatus fileStatus : fs.listStatus(new Path(topologyCatalogUri.getPath()), extensionFilter)) {
      URI topologySpecUri = new URI(Files.getNameWithoutExtension(fileStatus.getPath().getName()));
      Config topologyConfig = ConfigFactory.parseFile(new File(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
      Class specExecutorClass = Class.forName(topologyConfig.getString(ServiceConfigKeys.SPEC_EXECUTOR_KEY));
      SpecExecutor specExecutor = (SpecExecutor) GobblinConstructorUtils.invokeLongestConstructor(specExecutorClass, topologyConfig);

      TopologySpec.Builder topologySpecBuilder = TopologySpec
          .builder(topologySpecUri)
          .withConfig(topologyConfig)
          .withDescription("")
          .withVersion("1")
          .withSpecExecutor(specExecutor);

      TopologySpec topologySpec = topologySpecBuilder.build();
      topologySpecMap.put(topologySpecUri, topologySpec);
    }
    return topologySpecMap;
  }

  private FlowSpec createFlowSpec(String flowConfigResource, String source, String destination, boolean applyRetention, boolean applyRetentionOnInput)
      throws IOException, URISyntaxException {
    //Create a flow spec
    Properties flowProperties = new Properties();
    flowProperties.put(ConfigurationKeys.JOB_SCHEDULE_KEY, "* * * * *");
    flowProperties.put(ConfigurationKeys.FLOW_GROUP_KEY, "testFlowGroup");
    flowProperties.put(ConfigurationKeys.FLOW_NAME_KEY, "testFlowName");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, source);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, destination);
    flowProperties.put(ConfigurationKeys.FLOW_APPLY_RETENTION, Boolean.toString(applyRetention));
    flowProperties.put(ConfigurationKeys.FLOW_APPLY_INPUT_RETENTION, Boolean.toString(applyRetentionOnInput));
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
    FlowSpec spec = createFlowSpec("flow/flow1.conf", "LocalFS-1", "ADLS-1", false, false);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);
    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobDag.getEndNodes().size(), 1);

    //Get the 1st hop - Distcp from "LocalFS-1" to "HDFS-1"
    DagNode<JobExecutionPlan> startNode = jobDag.getStartNodes().get(0);
    JobExecutionPlan jobSpecWithExecutor = startNode.getValue();
    JobSpec jobSpec = jobSpecWithExecutor.getJobSpec();

    //Ensure the resolved job config for the first hop has the correct substitutions.
    Config jobConfig = jobSpec.getConfig();
    String flowGroup = "testFlowGroup";
    String flowName = "testFlowName";
    String expectedJobName1 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "Distcp", "LocalFS-1", "HDFS-1", "localToHdfs");
    String jobName1 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName1.startsWith(expectedJobName1));
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
    Assert.assertEquals(new Path(jobConfig.getString("gobblin.dataset.pattern")), new Path(from));
    Assert.assertEquals(jobConfig.getString("data.publisher.final.dir"), to);
    Assert.assertEquals(jobConfig.getString("type"), "java");
    Assert.assertEquals(jobConfig.getString("job.class"), "org.apache.gobblin.runtime.local.LocalJobLauncher");
    Assert.assertEquals(jobConfig.getString("launcher.type"), "LOCAL");
    //Ensure the spec executor has the correct configurations
    SpecExecutor specExecutor = jobSpecWithExecutor.getSpecExecutor();
    Assert.assertEquals(specExecutor.getUri().toString(), "fs:///");
    Assert.assertEquals(specExecutor.getClass().getCanonicalName(), "org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor");

    //Get the 2nd hop - "HDFS-1 to HDFS-1 : convert avro to json and encrypt". Ensure config has correct substitutions.
    Assert.assertEquals(jobDag.getChildren(startNode).size(), 1);
    DagNode<JobExecutionPlan> secondHopNode = jobDag.getChildren(startNode).get(0);
    jobSpecWithExecutor = secondHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    String expectedJobName2 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "ConvertToJsonAndEncrypt", "HDFS-1", "HDFS-1", "hdfsConvertToJsonAndEncrypt");
    String jobName2 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName2.startsWith(expectedJobName2));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName1);
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
    DagNode<JobExecutionPlan> thirdHopNode = jobDag.getChildren(secondHopNode).get(0);
    jobSpecWithExecutor = thirdHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    String expectedJobName3 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "Distcp", "HDFS-1", "HDFS-3", "hdfsToHdfs");
    String jobName3 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName3.startsWith(expectedJobName3));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName2);
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

    //Get the 4th hop - "Distcp from HDFS-3 to ADLS-1"
    Assert.assertEquals(jobDag.getChildren(thirdHopNode).size(), 1);
    DagNode<JobExecutionPlan> fourthHopNode = jobDag.getChildren(thirdHopNode).get(0);
    jobSpecWithExecutor = fourthHopNode.getValue();
    jobConfig = jobSpecWithExecutor.getJobSpec().getConfig();
    String expectedJobName4 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "DistcpToADL", "HDFS-3", "ADLS-1", "hdfsToAdl");
    String jobName4 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName4.startsWith(expectedJobName4));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName3);
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
  public void testCompileFlowWithRetention() throws URISyntaxException, IOException {
    FlowSpec spec = createFlowSpec("flow/flow1.conf", "LocalFS-1", "ADLS-1", true,
        true);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);
    Assert.assertEquals(jobDag.getNodes().size(), 9);
    Assert.assertEquals(jobDag.getStartNodes().size(), 2);
    Assert.assertEquals(jobDag.getEndNodes().size(), 5);

    String flowGroup = "testFlowGroup";
    String flowName = "testFlowName";

    List<DagNode<JobExecutionPlan>> currentHopNodes = jobDag.getStartNodes();

    List<String> expectedJobNames = Lists.newArrayList("SnapshotRetention", "Distcp", "SnapshotRetention", "ConvertToJsonAndEncrypt", "SnapshotRetention" ,
        "Distcp", "SnapshotRetention", "DistcpToADL", "SnapshotRetention");
    List<String> sourceNodes = Lists.newArrayList("LocalFS-1", "LocalFS-1", "HDFS-1", "HDFS-1", "HDFS-1", "HDFS-1", "HDFS-3", "HDFS-3", "ADLS-1");
    List<String> destinationNodes = Lists.newArrayList("LocalFS-1", "HDFS-1", "HDFS-1", "HDFS-1", "HDFS-1", "HDFS-3", "HDFS-3", "ADLS-1", "ADLS-1");
    List<String> edgeNames = Lists.newArrayList("localRetention", "localToHdfs", "hdfsRetention",
        "hdfsConvertToJsonAndEncrypt", "hdfsRetention", "hdfsToHdfs", "hdfsRetention", "hdfsToAdl", "hdfsRemoteRetention");

    List<DagNode<JobExecutionPlan>> nextHopNodes = new ArrayList<>();
    for (int i = 0; i < 9; i += 2) {
      if (i < 8) {
        Assert.assertEquals(currentHopNodes.size(), 2);
      } else {
        Assert.assertEquals(currentHopNodes.size(), 1);
      }
      Set<String> jobNames = new HashSet<>();
      jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
          join(flowGroup, flowName, expectedJobNames.get(i), sourceNodes.get(i), destinationNodes.get(i), edgeNames.get(i)));
      if (i < 8) {
        jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
            join(flowGroup, flowName, expectedJobNames.get(i + 1), sourceNodes.get(i + 1), destinationNodes.get(i + 1), edgeNames.get(i + 1)));
      }

      for (DagNode<JobExecutionPlan> dagNode : currentHopNodes) {
        Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
        String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
        Assert.assertTrue(jobNames.stream().anyMatch(jobName::startsWith));
        log.warn(jobName);
        nextHopNodes.addAll(jobDag.getChildren(dagNode));
      }

      currentHopNodes = nextHopNodes;
      nextHopNodes = new ArrayList<>();
    }
    Assert.assertEquals(nextHopNodes.size(), 0);

  }

  @Test (dependsOnMethods = "testCompileFlowWithRetention")
  public void testCompileFlowAfterFirstEdgeDeletion() throws URISyntaxException, IOException {
    //Delete the self edge on HDFS-1 that performs convert-to-json-and-encrypt.
    this.flowGraph.get().deleteFlowEdge("HDFS-1_HDFS-1_hdfsConvertToJsonAndEncrypt");

    FlowSpec spec = createFlowSpec("flow/flow1.conf", "LocalFS-1", "ADLS-1", false, false);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobDag.getEndNodes().size(), 1);

    //Get the 1st hop - Distcp from "LocalFS-1" to "HDFS-2"
    DagNode<JobExecutionPlan> startNode = jobDag.getStartNodes().get(0);
    JobExecutionPlan jobExecutionPlan = startNode.getValue();
    JobSpec jobSpec = jobExecutionPlan.getJobSpec();

    //Ensure the resolved job config for the first hop has the correct substitutions.
    Config jobConfig = jobSpec.getConfig();
    String flowGroup = "testFlowGroup";
    String flowName = "testFlowName";
    String expectedJobName1 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "Distcp", "LocalFS-1", "HDFS-2", "localToHdfs");
    String jobName1 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName1.startsWith(expectedJobName1));
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
    Assert.assertEquals(new Path(jobConfig.getString("gobblin.dataset.pattern")), new Path(from));
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
    DagNode<JobExecutionPlan> secondHopNode = jobDag.getChildren(startNode).get(0);
    jobExecutionPlan = secondHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();
    String expectedJobName2 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "ConvertToJsonAndEncrypt", "HDFS-2", "HDFS-2", "hdfsConvertToJsonAndEncrypt");
    String jobName2 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName2.startsWith(expectedJobName2));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName1);
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
    DagNode<JobExecutionPlan> thirdHopNode = jobDag.getChildren(secondHopNode).get(0);
    jobExecutionPlan = thirdHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();
    String expectedJobName3 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "Distcp", "HDFS-2", "HDFS-4", "hdfsToHdfs");
    String jobName3 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName3.startsWith(expectedJobName3));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName2);
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

    //Get the 4th hop - "Distcp from HDFS-4 to ADLS-1"
    Assert.assertEquals(jobDag.getChildren(thirdHopNode).size(), 1);
    DagNode<JobExecutionPlan> fourthHopNode = jobDag.getChildren(thirdHopNode).get(0);
    jobExecutionPlan = fourthHopNode.getValue();
    jobConfig = jobExecutionPlan.getJobSpec().getConfig();

    String expectedJobName4 = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join(flowGroup, flowName, "DistcpToADL", "HDFS-4", "ADLS-1", "hdfsToAdl");
    String jobName4 = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName4.startsWith(expectedJobName4));
    Assert.assertEquals(jobConfig.getString(ConfigurationKeys.JOB_DEPENDENCIES), jobName3);
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
    this.flowGraph.get().deleteFlowEdge("HDFS-2_HDFS-2_hdfsConvertToJsonAndEncrypt");

    FlowSpec spec = createFlowSpec("flow/flow1.conf", "LocalFS-1", "ADLS-1", false, false);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    //Ensure no path to destination.
    Assert.assertEquals(jobDag, null);
  }

  @Test (dependsOnMethods = "testCompileFlowAfterSecondEdgeDeletion")
  public void testCompileFlowSingleHop() throws IOException, URISyntaxException {
    FlowSpec spec = createFlowSpec("flow/flow2.conf", "HDFS-1", "HDFS-3", false, false);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);
    Assert.assertEquals(jobDag.getNodes().size(), 1);
    Assert.assertEquals(jobDag.getStartNodes().size(), 1);
    Assert.assertEquals(jobDag.getEndNodes().size(), 1);
    Assert.assertEquals(jobDag.getStartNodes().get(0), jobDag.getEndNodes().get(0));

    //Ensure hop is from HDFS-1 to HDFS-3 i.e. jobName == "testFlowGroup_testFlowName_Distcp_HDFS-1_HDFS-3".
    DagNode<JobExecutionPlan> dagNode = jobDag.getStartNodes().get(0);
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String expectedJobName = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "HDFS-1", "HDFS-3", "hdfsToHdfs");
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName.startsWith(expectedJobName));
  }


  @Test (dependsOnMethods = "testCompileFlowSingleHop")
  public void testMulticastPath() throws IOException, URISyntaxException {
    FlowSpec spec = createFlowSpec("flow/flow2.conf", "LocalFS-1", "HDFS-3,HDFS-4", false, false);
    Dag<JobExecutionPlan> jobDag = this.specCompiler.compileFlow(spec);

    Assert.assertEquals(jobDag.getNodes().size(), 4);
    Assert.assertEquals(jobDag.getEndNodes().size(), 2);
    Assert.assertEquals(jobDag.getStartNodes().size(), 2);

    //First hop must be from LocalFS to HDFS-1 and HDFS-2
    Set<String> jobNames = new HashSet<>();
    jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "LocalFS-1", "HDFS-1", "localToHdfs"));
    jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "LocalFS-1", "HDFS-2", "localToHdfs"));

    for (DagNode<JobExecutionPlan> dagNode : jobDag.getStartNodes()) {
      Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
      Assert.assertTrue(jobNames.stream().anyMatch(jobName::startsWith));
    }

    //Second hop must be from HDFS-1/HDFS-2 to HDFS-3/HDFS-4 respectively.
    jobNames = new HashSet<>();
    jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "HDFS-1", "HDFS-3", "hdfsToHdfs"));
    jobNames.add(Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "HDFS-2", "HDFS-4", "hdfsToHdfs"));
    for (DagNode<JobExecutionPlan> dagNode : jobDag.getStartNodes()) {
      List<DagNode<JobExecutionPlan>> nextNodes = jobDag.getChildren(dagNode);
      Assert.assertEquals(nextNodes.size(), 1);
      Config jobConfig = nextNodes.get(0).getValue().getJobSpec().getConfig();
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
      Assert.assertTrue(jobNames.stream().anyMatch(jobName::startsWith));
    }
  }

  @Test (dependsOnMethods = "testMulticastPath")
  public void testCompileMultiDatasetFlow() throws Exception {
    FlowSpec spec = createFlowSpec("flow/flow3.conf", "HDFS-1", "HDFS-3", true, false);

    Dag<JobExecutionPlan> dag = specCompiler.compileFlow(spec);

    // Should be 3 parallel jobs, one for each dataset, with copy -> retention
    Assert.assertEquals(dag.getNodes().size(), 6);
    Assert.assertEquals(dag.getEndNodes().size(), 3);
    Assert.assertEquals(dag.getStartNodes().size(), 3);

    String copyJobName = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "HDFS-1", "HDFS-3", "hdfsToHdfs");
    for (DagNode<JobExecutionPlan> dagNode : dag.getStartNodes()) {
      Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
      Assert.assertTrue(jobName.startsWith(copyJobName));
    }

    String retentionJobName = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "SnapshotRetention", "HDFS-3", "HDFS-3", "hdfsRetention");
    for (DagNode<JobExecutionPlan> dagNode : dag.getEndNodes()) {
      Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
      String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
      Assert.assertTrue(jobName.startsWith(retentionJobName));
    }
  }

  @Test (dependsOnMethods = "testCompileMultiDatasetFlow")
  public void testCompileCombinedDatasetFlow() throws Exception {
    FlowSpec spec = createFlowSpec("flow/flow4.conf", "HDFS-1", "HDFS-3", true, false);

    Dag<JobExecutionPlan> dag = specCompiler.compileFlow(spec);

    // Should be 2 jobs, each containing 3 datasets
    Assert.assertEquals(dag.getNodes().size(), 2);
    Assert.assertEquals(dag.getEndNodes().size(), 1);
    Assert.assertEquals(dag.getStartNodes().size(), 1);

    String copyJobName = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "Distcp", "HDFS-1", "HDFS-3", "hdfsToHdfs");
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName.startsWith(copyJobName));
    Assert.assertTrue(jobConfig.getString(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY).endsWith("{dataset0,dataset1,dataset2}"));

    String retentionJobName = Joiner.on(JobExecutionPlan.Factory.JOB_NAME_COMPONENT_SEPARATION_CHAR).
        join("testFlowGroup", "testFlowName", "SnapshotRetention", "HDFS-3", "HDFS-3", "hdfsRetention");
    Config jobConfig2 = dag.getEndNodes().get(0).getValue().getJobSpec().getConfig();
    String jobName2 = jobConfig2.getString(ConfigurationKeys.JOB_NAME_KEY);
    Assert.assertTrue(jobName2.startsWith(retentionJobName));
    Assert.assertTrue(jobConfig2.getString(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY).endsWith("{dataset0,dataset1,dataset2}"));
  }

  @Test (dependsOnMethods = "testCompileCombinedDatasetFlow")
  public void testUnresolvedFlow() throws Exception {
    FlowSpec spec = createFlowSpec("flow/flow5.conf", "HDFS-1", "HDFS-3", false, false);

    Dag<JobExecutionPlan> dag = specCompiler.compileFlow(spec);

    Assert.assertNull(dag);
    Assert.assertEquals(spec.getCompilationErrors().stream().map(c -> c.errorMessage).collect(Collectors.toSet()).size(), 1);
    spec.getCompilationErrors().stream().anyMatch(s -> s.errorMessage.contains(AzkabanProjectConfig.USER_TO_PROXY));
  }

  @Test (dependsOnMethods = "testUnresolvedFlow")
  public void testMissingSourceNodeError() throws Exception {
    FlowSpec spec = createFlowSpec("flow/flow5.conf", "HDFS-NULL", "HDFS-3", false, false);

    Dag<JobExecutionPlan> dag = specCompiler.compileFlow(spec);

    Assert.assertEquals(dag, null);
    Assert.assertEquals(spec.getCompilationErrors().size(), 1);
    spec.getCompilationErrors().stream().anyMatch(s -> s.errorMessage.contains("Flowgraph does not have a node with id"));
  }

  @Test (dependsOnMethods = "testMissingSourceNodeError")
  public void testMissingDestinationNodeError() throws Exception {
    FlowSpec spec = createFlowSpec("flow/flow5.conf", "HDFS-1", "HDFS-NULL", false, false);

    Dag<JobExecutionPlan> dag = specCompiler.compileFlow(spec);

    Assert.assertNull(dag);
    Assert.assertEquals(spec.getCompilationErrors().size(), 1);
    spec.getCompilationErrors().stream().anyMatch(s -> s.errorMessage.contains("Flowgraph does not have a node with id"));
  }

  private String formNodeFilePath(File flowGraphDir, String groupDir, String fileName) {
    return flowGraphDir.getName() + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  private void cleanUpDir(String dir) throws IOException {
    File dirToDelete = new File(dir);
    if (dirToDelete.exists()) {
      FileUtils.deleteDirectory(new File(dir));
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    cleanUpDir(TESTDIR);
    try {
      this.specCompiler.getServiceManager().stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.warn("Could not stop Service Manager");
    }
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
    public Future<? extends SpecProducer<Spec>> getProducer() {
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