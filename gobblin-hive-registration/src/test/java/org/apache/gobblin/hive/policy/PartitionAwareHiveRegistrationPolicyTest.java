package org.apache.gobblin.hive.policy;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.HiveTable;

import static org.apache.gobblin.hive.policy.PartitionAwareHiveRegistrationPolicy.HIVE_PARTITION_REGEX;
import static org.apache.gobblin.hive.policy.PartitionAwareHiveRegistrationPolicy.HIVE_TABLE_PARTITION_KEYS;
import static org.mockito.Mockito.mock;


public class PartitionAwareHiveRegistrationPolicyTest {
  private Path path;

  @Test
  public void testGetTable()
      throws Exception {

    State state = new State();
    state.appendToListProp(HIVE_PARTITION_REGEX,"(s3://testbucket/myawesomelogs/compacted/)dt=(.*)/hr=(.*)");
    state.appendToListProp(HIVE_TABLE_PARTITION_KEYS, "dt,hr");
    this.path = new Path(getClass().getResource("/test-hive-table").toString());

    PartitionAwareHiveRegistrationPolicy policy = new PartitionAwareHiveRegistrationPolicy(state);
    HiveTable table = policy.getTable(path, "testDb", "testTable");

    Assert.assertEquals(table.getPartitionKeys().size(), 2);
    Assert.assertEquals(table.getPartitionKeys().get(0).getName(), "dt");
    Assert.assertEquals(table.getPartitionKeys().get(0).getType(), "string");
    Assert.assertEquals(table.getPartitionKeys().get(1).getName(), "hr");
    Assert.assertEquals(table.getPartitionKeys().get(1).getType(), "string");
  }

  @Test
  public void testGetPartition()
      throws Exception {

    HiveSerDeManager mockHiveSerDeManager = mock(HiveSerDeManager.class);

    HiveTable table = new HiveTable.Builder().withDbName("test").withTableName("test").withSerdeManaager(mockHiveSerDeManager).build();
    State state = new State();
    state.appendToListProp(HIVE_PARTITION_REGEX,"(s3://testbucket/myawesomelogs/compacted/)dt=(.*)/hr=(.*)");

    this.path = new Path("s3://testbucket/myawesomelogs/compacted/dt=20170101/hr=22/");

    PartitionAwareHiveRegistrationPolicy policy = new PartitionAwareHiveRegistrationPolicy(state);

    HivePartition partition = policy.getPartition(path, table).orNull();

    Assert.assertEquals(partition.getValues().size(), 2);
    Assert.assertEquals(partition.getValues().get(0), "20170101");
    Assert.assertEquals(partition.getValues().get(1), "22");
  }

  @Test
  public void testGetTableLocationWithTableRegexp()
      throws Exception {

    State state = new State();
    state.appendToListProp(HIVE_PARTITION_REGEX,"(s3://testbucket/myawesomelogs/compacted/)dt=(.*)/hr=(.*)");

    this.path = new Path("s3://testbucket/myawesomelogs/compacted/dt=20170101/hr=22/");

    PartitionAwareHiveRegistrationPolicy policy = new PartitionAwareHiveRegistrationPolicy(state);

    Path tableLocation = policy.getTableLocation(path);

    Assert.assertEquals(tableLocation, new Path("s3://testbucket/myawesomelogs/compacted/"));
  }

  @Test
  public void testGetTableLocationWithoutTableRegexp()
      throws Exception {

    State state = new State();

    this.path = new Path("s3://testbucket/myawesomelogs/compacted/");

    PartitionAwareHiveRegistrationPolicy policy = new PartitionAwareHiveRegistrationPolicy(state);

    Path tableLocation = policy.getTableLocation(path);

    Assert.assertEquals(tableLocation, new Path("s3://testbucket/myawesomelogs/compacted/"));
  }

}