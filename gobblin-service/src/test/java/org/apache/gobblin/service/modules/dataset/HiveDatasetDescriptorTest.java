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


public class HiveDatasetDescriptorTest {

  @Test
  public void objectCreation() throws IOException {
    Config baseConfig = ConfigFactory.empty().withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("hive"))
        .withValue(DatasetDescriptorConfigKeys.DATABASE_KEY, ConfigValueFactory.fromAnyRef("testDb_Db1"))
        .withValue(DatasetDescriptorConfigKeys.TABLE_KEY, ConfigValueFactory.fromAnyRef("testTable_Table1"));;

    SqlDatasetDescriptor descriptor1 = new HiveDatasetDescriptor(baseConfig.withValue(HiveDatasetDescriptor.IS_PARTITIONED_KEY, ConfigValueFactory.fromAnyRef(true)));
    SqlDatasetDescriptor descriptor2 = new HiveDatasetDescriptor(baseConfig.withValue(HiveDatasetDescriptor.IS_PARTITIONED_KEY, ConfigValueFactory.fromAnyRef(false)));

    Assert.assertNotEquals(descriptor1, descriptor2);
  }
}