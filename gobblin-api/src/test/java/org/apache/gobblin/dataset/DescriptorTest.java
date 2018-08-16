package org.apache.gobblin.dataset;

import org.testng.Assert;
import org.testng.annotations.Test;


public class DescriptorTest {

  @Test
  public void testDatasetDescriptor() {
    DatasetDescriptor dataset = new DatasetDescriptor("hdfs", "/data/tracking/PageViewEvent");
    dataset.addMetadata("fsUri", "hdfs://test.com:2018");

    DatasetDescriptor copy = dataset.copy();
    Assert.assertEquals(copy.getName(), dataset.getName());
    Assert.assertEquals(copy.getPlatform(), dataset.getPlatform());
    Assert.assertEquals(copy.getMetadata(), dataset.getMetadata());
    Assert.assertEquals(dataset, copy);
    Assert.assertEquals(dataset.hashCode(), copy.hashCode());
  }

  @Test
  public void testPartitionDescriptor() {
    // Test serialization
    String partitionJson = "{\"clazz\":\"org.apache.gobblin.dataset.PartitionDescriptor\",\"data\":{\"dataset\":{\"platform\":\"hdfs\",\"metadata\":{},\"name\":\"/data/tracking/PageViewEvent\"},\"name\":\"hourly/2018/08/14/18\"}}";

    DatasetDescriptor dataset = new DatasetDescriptor("hdfs", "/data/tracking/PageViewEvent");
    String partitionName = "hourly/2018/08/14/18";
    PartitionDescriptor partition = new PartitionDescriptor(partitionName, dataset);
    Assert.assertEquals(Descriptor.serialize(partition), partitionJson);
    System.out.println(partitionJson);

    Descriptor partition2 = Descriptor.deserialize(partitionJson);
    Assert.assertEquals(partition2.getName(), partition.getName());
    Assert.assertEquals(((PartitionDescriptor)partition2).getDataset(), partition.getDataset());
    Assert.assertEquals(partition, partition2);
    Assert.assertEquals(partition.hashCode(), partition2.hashCode());

    // Test copy
    PartitionDescriptor partition3 = partition.copy();
    Assert.assertEquals(partition3.getDataset(), dataset);
    Assert.assertEquals(partition3.getName(), partitionName);
  }
}
