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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;


public class IcebergDatasetDescriptorTest {
  Config baseConfig = createDatasetDescriptorConfig("iceberg","testDb_Db1", "testTable_Table1");

  @Test
  public void testIsPathContaining() throws IOException {
    Config config1 = createDatasetDescriptorConfig("iceberg","testDb_Db1", "testTable_Table1");
    Config config2 = createDatasetDescriptorConfig("iceberg","testDb_Db1", "testTable_Table2");

    IcebergDatasetDescriptor current = new IcebergDatasetDescriptor(baseConfig);
    IcebergDatasetDescriptor other = new IcebergDatasetDescriptor(config1);
    IcebergDatasetDescriptor yetAnother = new IcebergDatasetDescriptor(config2);

    Assert.assertTrue(current.isPathContaining(other));
    Assert.assertFalse(current.isPathContaining(yetAnother));

  }
  private Config createDatasetDescriptorConfig(String platform, String dbName, String tableName) {
    return ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef(platform))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef(dbName))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef(tableName));
  }
}
