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

import org.junit.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;

import static org.testng.Assert.*;


public class SqlDatasetDescriptorTest {

  @Test
  public void testContains() throws IOException {
    Config config1 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("sqlserver"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_Db1"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_Table1"));

    SqlDatasetDescriptor descriptor1 = new SqlDatasetDescriptor(config1);

    Config config2 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("sqlserver"));
    SqlDatasetDescriptor descriptor2 = new SqlDatasetDescriptor(config2);
    Assert.assertTrue(descriptor2.contains(descriptor1));

    Config config3 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("sqlserver"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_.*"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_.*"));

    SqlDatasetDescriptor descriptor3 = new SqlDatasetDescriptor(config3);
    Assert.assertTrue(descriptor3.contains(descriptor1));

    Config config4 = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("sqlserver"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("Db_.*"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("Table_.*"));
    SqlDatasetDescriptor descriptor4 = new SqlDatasetDescriptor(config4);
    Assert.assertFalse(descriptor4.contains(descriptor1));
  }
}