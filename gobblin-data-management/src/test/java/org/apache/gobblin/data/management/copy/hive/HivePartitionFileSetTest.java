package org.apache.gobblin.data.management.copy.hive;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.util.TestUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;


public class HivePartitionFileSetTest {
  @Mock HiveCopyEntityHelper helper;
  @Mock Partition partition;
  @Mock HiveDataset dataset;
  @Mock FileSystem fileSystem;
  @Mock org.apache.hadoop.hive.metastore.api.Partition tPartition;
  @Mock StorageDescriptor storageDescriptor;
  Properties properties = newPropertiesWithRequiredFields();

  @BeforeMethod
  public void setup() {
    initMocks(this);
    when(helper.getDataset()).thenReturn(dataset);
    when(helper.getTargetTable()).thenReturn(TestUtils.createTestTable());
    when(helper.getTargetFs()).thenReturn(fileSystem);

    when(partition.getCompleteName()).thenReturn("A dummy partition for testing");
    when(partition.getTPartition()).thenReturn(tPartition);
    when(partition.getDataLocation()).thenReturn(TestUtils.createTestPath());

    when(tPartition.getSd()).thenReturn(storageDescriptor);
    when(tPartition.deepCopy()).thenReturn(tPartition);

    when(storageDescriptor.getSerdeInfo()).thenReturn(new SerDeInfo("Fake Serializer", "Fake Version",
        Collections.emptyMap()));

    when(helper.getTargetLocation(eq(fileSystem), eq(TestUtils.createTestPath()), any(Optional.class)))
        .thenReturn(TestUtils.createTestPath());
  }

  @Test(expectedExceptions = IOException.class)
  public void shouldFailIfExistingEntityPolicyIsAbortAndEntityExistsAndPartialSuccessNotAllowed() throws IOException {
    List<String> partitions = ImmutableList.of("foo");
    // Given an existing entity policy of abort
    when(helper.getExistingEntityPolicy()).thenReturn(HiveCopyEntityHelper.ExistingEntityPolicy.ABORT);
    when(helper.getConfiguration()).thenReturn(CopyConfiguration.builder(fileSystem, properties).build());
    when(partition.getValues()).thenReturn(partitions);
    // And an existing entity
    when(helper.getTargetPartitions()).thenReturn(ImmutableMap.of(partitions, partition));
    HivePartitionFileSet fileSet = new HivePartitionFileSet(helper, partition, properties);
    // When generating the copy entities
    fileSet.generateCopyEntities();
    // Then we should have failed with an exception
    Assert.fail("Should have failed with an IOException due to existing entity.");
  }

  @Test
  public void shouldReturnEmptyIfExistingEntityPolicyIsAbortAndEntityExistsAndPartialSuccessAllowed() throws IOException {
    List<String> partitions = ImmutableList.of("foo");
    Properties properties = newPropertiesWithRequiredFields();
    properties.put(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS.toString());
    when(helper.getExistingEntityPolicy()).thenReturn(HiveCopyEntityHelper.ExistingEntityPolicy.ABORT);
    when(helper.getConfiguration()).thenReturn(CopyConfiguration.builder(fileSystem, properties).build());
    when(partition.getValues()).thenReturn(partitions);
    when(helper.getTargetPartitions()).thenReturn(ImmutableMap.of(partitions, partition));
    HivePartitionFileSet fileSet = new HivePartitionFileSet(helper, partition, properties);
    Assert.assertEquals(fileSet.generateCopyEntities(), Collections.emptyList());
  }

  private static Properties newPropertiesWithRequiredFields() {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/tmp");
    return properties;
  }
}
