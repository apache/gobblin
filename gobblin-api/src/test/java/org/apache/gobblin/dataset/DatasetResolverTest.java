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

package org.apache.gobblin.dataset;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;


public class DatasetResolverTest {

  @Test
  public void testAsDescriptorResolver() {
    DescriptorResolver resolver = new TestDatasetResolver();
    State state = new State();

    // Test dataset descriptor
    DatasetDescriptor dataset = new DatasetDescriptor("hdfs", "/data/tracking/PageViewEvent");
    Descriptor descriptor = resolver.resolve(dataset, state);
    Assert.assertTrue(descriptor.getClass().isAssignableFrom(DatasetDescriptor.class));
    Assert.assertEquals(descriptor.getName(), TestDatasetResolver.DATASET_NAME);

    // Test partition descriptor
    String partitionName = "hourly/2018/08/14/18";
    PartitionDescriptor partition = new PartitionDescriptor(partitionName, dataset);
    descriptor = resolver.resolve(partition, state);
    Assert.assertTrue(descriptor.getClass().isAssignableFrom(DatasetDescriptor.class));
    Assert.assertEquals(descriptor.getName(), TestDatasetResolver.DATASET_NAME);

    // Test unsupported descriptor
    Assert.assertEquals(resolver.resolve(new MockDescriptor("test"), state), null);
  }

  private static class TestDatasetResolver implements DatasetResolver {
    static final String DATASET_NAME = "TEST";

    @Override
    public DatasetDescriptor resolve(DatasetDescriptor raw, State state) {
      DatasetDescriptor descriptor = new DatasetDescriptor(raw.getPlatform(), DATASET_NAME);
      raw.getMetadata().forEach(descriptor::addMetadata);
      return descriptor;
    }
  }

  private static class MockDescriptor extends Descriptor {
    public MockDescriptor(String name) {
      super(name);
    }
  }
}
