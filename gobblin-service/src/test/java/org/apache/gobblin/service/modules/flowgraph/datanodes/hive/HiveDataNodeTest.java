package org.apache.gobblin.service.modules.flowgraph.datanodes.hive;

import java.net.URI;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;

import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


public class HiveDataNodeTest {

  Config config = null;

  @BeforeMethod
  public void setUp() {
    String expectedNodeId = "some-node-id";
    String expectedAdlFsUri = "abfs://data@adl.dfs.core.windows.net";
    String expectedHiveMetastoreUri = "thrift://hcat.company.com:7552";

    config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(expectedNodeId))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(expectedHiveMetastoreUri));
  }

  @AfterMethod
  public void tearDown() {
  }


  @Test
  public void testIsMetastoreUriValid() throws Exception {
    HiveDataNode hiveDataNode = new HiveDataNode(config);
    Assert.assertNotNull(hiveDataNode);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIsMetastoreUriInValid() throws Exception {
    String expectedHiveMetastoreUri = "thrift-1://hcat.company.com:7552";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(expectedHiveMetastoreUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
  }

  @Test
  public void testIsUriValidHdfs() throws Exception{
    String expectedHdfsFsUri = "hdfs://hdfs.company.com:9000";

    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedHdfsFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
    boolean isValidFs = hiveDataNode.isUriValid(new URI(hiveDataNode.getFsUri()));
    Assert.assertTrue(isValidFs);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIsUriInValidHdfs() throws Exception{
    String expectedHdfsFsUri = "hdf://hdfs.company.com:9000";

    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedHdfsFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
  }

  @Test
  public void testIsUriValidAdl() throws Exception{
    String expectedAdlFsUri = "adl://azure-adl.azuredatalake.net/";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
    boolean isValidFs = hiveDataNode.isUriValid(new URI(hiveDataNode.getFsUri()));
    Assert.assertTrue(isValidFs);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIsUriInValidAdl() throws Exception{
    String expectedAdlFsUri = "adlss://azure-adl.azuredatalake.net/";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);;
  }

  @Test
  public void testIsUriValidAdls() throws Exception{
    String expectedAdlFsUri = "abfs://data@adl.dfs.core.windows.net";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
    boolean isValidFs = hiveDataNode.isUriValid(new URI(hiveDataNode.getFsUri()));
    Assert.assertTrue(isValidFs);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIsUriInValidAdls() throws Exception{
    String expectedAdlFsUri = "abfs2://data@adl.dfs.core.windows.net";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);

  }
}