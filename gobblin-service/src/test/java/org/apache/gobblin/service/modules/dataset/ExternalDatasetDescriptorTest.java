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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.junit.Assert;
import org.testng.annotations.Test;


public class ExternalDatasetDescriptorTest {

  @Test
  public void testContains() throws IOException {
    Config config1 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("external"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/b"));
    ExternalDatasetDescriptor descriptor1 = new ExternalDatasetDescriptor(config1);

    // Verify that same path points to same dataset
    Config config2 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("external"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/b"));
    ExternalDatasetDescriptor descriptor2 = new ExternalDatasetDescriptor(config2);
    Assert.assertEquals(descriptor2.contains(descriptor1).size(), 0);

    // Verify that different path points to different dataset
    Config config3 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("external"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/c"));
    ExternalDatasetDescriptor descriptor3 = new ExternalDatasetDescriptor(config3);
    Assert.assertNotEquals(descriptor3.contains(descriptor1).size(), 0);

  }
}