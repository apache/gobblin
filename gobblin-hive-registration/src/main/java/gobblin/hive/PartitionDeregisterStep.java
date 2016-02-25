package gobblin.hive;

import lombok.AllArgsConstructor;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Optional;

import gobblin.commit.CommitStep;
import gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import gobblin.hive.metastore.HiveMetaStoreUtils;


/**
 * {@link CommitStep} to deregister a Hive partition.
 */
@AllArgsConstructor
public class PartitionDeregisterStep implements CommitStep {

  private Table table;
  private Partition partition;
  private final Optional<String> metastoreURI;
  private final HiveRegProps props;

  @Override public boolean isCompleted() throws IOException {
    return false;
  }

  @Override public void execute() throws IOException {
    HiveTable hiveTable = HiveMetaStoreUtils.getHiveTable(this.table);
    HiveRegister hiveRegister = new HiveMetaStoreBasedRegister(this.props, this.metastoreURI);
    hiveRegister.dropPartitionIfExists(this.partition.getDbName(), this.partition.getTableName(),
        hiveTable.getPartitionKeys(), this.partition.getValues());
  }
}
