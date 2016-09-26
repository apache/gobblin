package gobblin.data.management.copy.hive;

import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.partition.FileSet;

import lombok.Getter;


/**
 * A {@link FileSet} for Hive datasets. Contains information on Hive table.
 */
@Getter
public abstract class HiveFileSet extends FileSet<CopyEntity> {

  private final Table table;
  private final HiveDataset hiveDataset;

  public HiveFileSet(String name, HiveDataset dataset) {
    super(name, dataset);
    this.table = dataset.getTable();
    this.hiveDataset = dataset;
  }
}
