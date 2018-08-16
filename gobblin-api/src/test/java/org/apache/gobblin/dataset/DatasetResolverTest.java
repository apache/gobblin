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
