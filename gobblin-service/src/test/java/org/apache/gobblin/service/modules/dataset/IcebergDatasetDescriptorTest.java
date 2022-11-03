package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;


public class IcebergDatasetDescriptorTest {
  Config baseConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("iceberg"))
      .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_Db1"))
      .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_Table1"));

  @Test
  public void testIsPathContaining() throws IOException {
    Config config1 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("iceberg"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_Db1"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_Table1"));

    Config config2 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("iceberg"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_Db2"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_Table2"));

    IcebergDatasetDescriptor current = new IcebergDatasetDescriptor(baseConfig);
    IcebergDatasetDescriptor other = new IcebergDatasetDescriptor(config1);
    IcebergDatasetDescriptor yetAnother = new IcebergDatasetDescriptor(config2);

    Assert.assertTrue(current.isPathContaining(other));
    Assert.assertFalse(current.isPathContaining(yetAnother));

  }
}
