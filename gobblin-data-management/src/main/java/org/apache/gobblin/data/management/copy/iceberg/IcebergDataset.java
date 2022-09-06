package org.apache.gobblin.data.management.copy.iceberg;

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import lombok.Getter;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.catalog.TableIdentifier;


@Getter
public class IcebergDataset implements PrioritizedCopyableDataset {
  private final String dbName;
  private final String inputTableName;
  private IcebergTable icebergTable;
  @Getter
  protected Properties properties;
  @Getter
  protected FileSystem fs;

  public IcebergDataset(String db, String table, IcebergTable icebergTbl, Properties properties, FileSystem fs) {
    this.dbName = db;
    this.inputTableName = table;
    this.icebergTable = icebergTbl;
    this.properties = properties;
    this.fs = fs;

  }

  public IcebergDataset(String db, String table) {
    this.dbName = db;
    this.inputTableName = table;
  }

  @Override
  public String datasetURN() {
    return this.dbName + "." + this.inputTableName;
  }

  public static IcebergDataset of(String dbName, String tableName) {
    return new IcebergDataset(dbName, tableName);
  }

  public static IcebergDataset of(TableIdentifier identifier) {
    return new IcebergDataset(identifier.namespace().toString(), identifier.name());
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    return new IcebergCopyEntityHelper(this, configuration, targetFs).getCopyEntities(configuration);
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration,
      Comparator<FileSet<CopyEntity>> prioritizer, PushDownRequestor<FileSet<CopyEntity>> requestor)
      throws IOException {
    return new IcebergCopyEntityHelper(this, configuration, targetFs).getCopyEntities(configuration);
  }
}
