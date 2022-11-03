package org.apache.gobblin.service.modules.flowgraph.datanodes.iceberg;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;


public class IcebergDataNodeTest {

  Config config = null;

  @BeforeMethod
  public void setUp() {
    String expectedNodeId = "some-iceberg-node-id";
    String expectedAdlFsUri = "hdfs://data.hdfs.core.windows.net";
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
  public void testIcebergDataNodeWithValidMetastoreUri() throws DataNode.DataNodeCreationException, URISyntaxException {
    IcebergDataNode icebergDataNode = new IcebergDataNode(config);
    URI uri = new URI(config.getString(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri"));
    Assert.assertTrue(icebergDataNode.isMetastoreUriValid(uri));
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIcebergDataNodeWithInvalidMetastoreUri() throws DataNode.DataNodeCreationException, URISyntaxException {
    String expectedHiveMetastoreUri = "thrift-1://hcat.company.com:7552";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(expectedHiveMetastoreUri));
    IcebergDataNode icebergDataNode = new IcebergDataNode(config);
    URI uri = new URI(config.getString(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri"));
    icebergDataNode.isMetastoreUriValid(uri);
  }
}
