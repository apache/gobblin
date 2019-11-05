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

public class HttpDatasetDescriptorTest {

  @Test
  public void testContains() throws IOException {
    Config config1 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("https"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/b"));
    HttpDatasetDescriptor descriptor1 = new HttpDatasetDescriptor(config1);

    // Verify that same path points to same dataset
    Config config2 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("https"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/b"));
    HttpDatasetDescriptor descriptor2 = new HttpDatasetDescriptor(config2);
    Assert.assertTrue(descriptor2.contains(descriptor1));

    // Verify that same path but different platform points to different dataset
    Config config3 = ConfigFactory.empty()
        .withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef("http"))
        .withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef("https://a.com/b"));
    HttpDatasetDescriptor descriptor3 = new HttpDatasetDescriptor(config3);
    Assert.assertFalse(descriptor3.contains(descriptor1));

  }
}