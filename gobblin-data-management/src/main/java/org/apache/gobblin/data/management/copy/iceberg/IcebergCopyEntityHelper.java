package org.apache.gobblin.data.management.copy.iceberg;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;
import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveTargetPathHelper;
import org.apache.gobblin.data.management.copy.hive.UnpartitionedTableFileSet;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;


@Slf4j
@Getter
public class IcebergCopyEntityHelper {

  private final IcebergDataset icebergDataset;
  private final CopyConfiguration configuration; //TODO initialize the copy configuration properly
  private FileSystem targetFs; // initialize target fs properly

  private final Optional<String> sourceMetastoreURI;
  private final Optional<String> targetMetastoreURI;
  private final String targetDatabase;
  /** Target metastore URI */
  public static final String TARGET_METASTORE_URI_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.metastore.uri";
  /** Target database name */
  public static final String TARGET_DATABASE_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.database";

  /**
   * Represents a source {@link FileStatus} and a {@link Path} destination.
   */
  @Data
  private static class SourceAndDestination {
    private final FileStatus source;
    private final Path destination;
  }

  public IcebergCopyEntityHelper(IcebergDataset icebergDataset, CopyConfiguration configuration, FileSystem targetFs) {
    this.icebergDataset = icebergDataset;
    this.configuration = configuration;
    this.targetFs = targetFs;

    this.sourceMetastoreURI =
        Optional.fromNullable(this.icebergDataset.getProperties().getProperty(HiveDatasetFinder.HIVE_METASTORE_URI_KEY));
    this.targetMetastoreURI =
        Optional.fromNullable(this.icebergDataset.getProperties().getProperty(TARGET_METASTORE_URI_KEY));
    this.targetDatabase = Optional.fromNullable(this.icebergDataset.getProperties().getProperty(TARGET_DATABASE_KEY))
        .or(this.icebergDataset.getDbName());
  }

  /**
   * See {@link #getCopyEntities(CopyConfiguration, Comparator, PushDownRequestor)}. This method does not pushdown any prioritizer.
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities(CopyConfiguration configuration) throws IOException {
    FileSet<CopyEntity> fileSet = new IcebergTableFileSet(this.icebergDataset.getInputTableName(), this.icebergDataset, this);
    return Iterators.singletonIterator(fileSet);  }

  /**
   * Finds all files read by the table and generates {@link CopyEntity}s for duplicating the table. The semantics are as follows:
   * 1. Find all valid {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}. If the table is partitioned, the
   *    {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of the base
   *    table will be ignored, and we will instead process the {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of each partition.
   * 2. For each {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} find all files referred by it.
   * 3. Generate a {@link CopyableFile} for each file referred by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   * 4. If the table is partitioned, create a file set for each partition.
   * 5. Create work units for registering, deregistering partitions / tables, and deleting unnecessary files in the target.
   *
   * For computation of target locations see {@link HiveTargetPathHelper#getTargetPath}
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities(CopyConfiguration configuration, Comparator<FileSet<CopyEntity>> prioritizer,
      PushDownRequestor<FileSet<CopyEntity>> requestor) throws IOException {
      return null;
  }

  Map<Path, FileStatus> getPath() throws IOException {
    Map<Path, FileStatus> result = Maps.newHashMap();
    IcebergTable icebergTable = this.icebergDataset.getIcebergTable();
    IcebergSnapshotInfo icebergSnapshotInfo = icebergTable.getCurrentSnapshotInfo();

    List<String> pathsToCopy = icebergSnapshotInfo.getAllPaths();

    for(String pathString : pathsToCopy){
      Path path = new Path(pathString);
      result.put(path, this.targetFs.getFileStatus(path));
    }

    return result;
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   */
  List<CopyableFile.Builder> getCopyableFilesFromPaths(Map<Path, FileStatus> paths,
      CopyConfiguration configuration) throws IOException {

    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();

    Configuration hadoopConfiguration = new Configuration();
    FileSystem actualSourceFs;


    for(Map.Entry<Path, FileStatus> entry : paths.entrySet()){
      dataFiles.add(new SourceAndDestination(entry.getValue(), this.targetFs.makeQualified(entry.getKey())));
    }

    for(SourceAndDestination sourceAndDestination : dataFiles){
      URI uri = sourceAndDestination.getSource().getPath().toUri();
      actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(hadoopConfiguration);

      // TODO source and destination filesystems needs to be verified
      List<OwnerAndPermission> ancestorOwnerAndPermission =
          CopyableFile.resolveReplicatedOwnerAndPermissionsRecursively(actualSourceFs,
              sourceAndDestination.getSource().getPath().getParent(), sourceAndDestination.getDestination().getParent(), configuration);

      builders.add(CopyableFile.fromOriginAndDestination(actualSourceFs, sourceAndDestination.getSource(),
          sourceAndDestination.getDestination(), configuration).
          ancestorsOwnerAndPermission(ancestorOwnerAndPermission));
    }
    return builders;
  }

  DatasetDescriptor getSourceDataset() {
    String sourceTable = icebergDataset.getDbName() + "." + icebergDataset.getInputTableName();

    URI hiveMetastoreURI = null;
    if (sourceMetastoreURI.isPresent()) {
      hiveMetastoreURI = URI.create(sourceMetastoreURI.get());
    }

    DatasetDescriptor sourceDataset =
        new DatasetDescriptor(DatasetConstants.PLATFORM_HIVE, hiveMetastoreURI, sourceTable);
    sourceDataset.addMetadata(DatasetConstants.FS_URI, IcebergDataset.getFs().getUri().toString());
    return sourceDataset;
  }

  DatasetDescriptor getDestinationDataset() {
    String destinationTable = this.getTargetDatabase() + "." + this.getIcebergDataset().getInputTableName();

    URI hiveMetastoreURI = null;
    if (targetMetastoreURI.isPresent()) {
      hiveMetastoreURI = URI.create(targetMetastoreURI.get());
    }

    DatasetDescriptor destinationDataset =
        new DatasetDescriptor(DatasetConstants.PLATFORM_HIVE, hiveMetastoreURI, destinationTable);
    destinationDataset.addMetadata(DatasetConstants.FS_URI, this.getTargetFs().getUri().toString());
    return destinationDataset;
  }
}
