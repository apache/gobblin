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

package org.apache.gobblin.data.management.copy.iceberg;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Properties;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Iceberg dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Getter
public class IcebergDataset implements PrioritizedCopyableDataset {
  private final String dbName;
  private final String inputTableName;
  private IcebergTable icebergTable;
  protected Properties properties;
  protected FileSystem fs;

  private Optional<String> sourceMetastoreURI;
  private Optional<String> targetMetastoreURI;

  /** Target metastore URI */
  public static final String TARGET_METASTORE_URI_KEY =
      IcebergDatasetFinder.ICEBERG_DATASET_PREFIX + ".copy.target.metastore.uri";
  /** Target database name */
  public static final String TARGET_DATABASE_KEY = IcebergDatasetFinder.ICEBERG_DATASET_PREFIX + ".copy.target.database";

  public IcebergDataset(String db, String table, IcebergTable icebergTbl, Properties properties, FileSystem fs) {
    this.dbName = db;
    this.inputTableName = table;
    this.icebergTable = icebergTbl;
    this.properties = properties;
    this.fs = fs;
    this.sourceMetastoreURI =
        Optional.fromNullable(this.properties.getProperty(IcebergDatasetFinder.ICEBERG_HIVE_CATALOG_METASTORE_URI_KEY));
    this.targetMetastoreURI =
        Optional.fromNullable(this.properties.getProperty(TARGET_METASTORE_URI_KEY));
  }

  public IcebergDataset(String db, String table) {
    this.dbName = db;
    this.inputTableName = table;
  }

  /**
   * Represents a source {@link FileStatus} and a {@link Path} destination.
   */
  @Data
  private static class SourceAndDestination {
    private final FileStatus source;
    private final Path destination;
  }

  @Override
  public String datasetURN() {
    // TODO: verify!
    return this.dbName + "." + this.inputTableName;
  }

  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link #getCopyEntities}.
   */
  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration) {
    return getCopyEntities(configuration);
  }
  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link #getCopyEntities}.
   */
  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration,
      Comparator<FileSet<CopyEntity>> prioritizer, PushDownRequestor<FileSet<CopyEntity>> requestor) {
    // TODO: Implement PushDownRequestor and priority based copy entity iteration
    return getCopyEntities(configuration);
  }

  /**
   * Finds all files read by the table and generates {@link CopyEntity}s for duplicating the table.
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities(CopyConfiguration configuration) {
    FileSet<CopyEntity> fileSet = new IcebergTableFileSet(this.getInputTableName(), this, configuration);
    return Iterators.singletonIterator(fileSet);  }

  /**
   * Finds all files read by the table file set and generates {@link CopyEntity}s for duplicating the table.
   */
  @VisibleForTesting
  Collection<CopyEntity> generateCopyEntities(CopyConfiguration configuration) throws IOException {
    String fileSet = this.getInputTableName();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    log.info("Fetching all the files to be copied");
    Map<Path, FileStatus> mapOfPathsToCopy = getFilePaths();

    log.info("Fetching copyable file builders from their respective file sets and adding to collection of copy entities");
    for (CopyableFile.Builder builder : getCopyableFilesFromPaths(mapOfPathsToCopy, configuration)) {
      CopyableFile fileEntity =
          builder.fileSet(fileSet).datasetOutputPath(this.fs.getUri().getPath()).build();
      fileEntity.setSourceData(getSourceDataset());
      fileEntity.setDestinationData(getDestinationDataset());
      copyEntities.add(fileEntity);
    }
    return copyEntities;
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   */
  protected List<CopyableFile.Builder> getCopyableFilesFromPaths(Map<Path, FileStatus> paths, CopyConfiguration configuration) throws IOException {

    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();
    Configuration hadoopConfiguration = new Configuration();
    FileSystem actualSourceFs;

    for(Map.Entry<Path, FileStatus> entry : paths.entrySet()) {
      dataFiles.add(new SourceAndDestination(entry.getValue(), this.fs.makeQualified(entry.getKey())));
    }

    for(SourceAndDestination sourceAndDestination : dataFiles) {
      actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(hadoopConfiguration);

      // TODO Add ancestor owner and permissions in future releases
      builders.add(CopyableFile.fromOriginAndDestination(actualSourceFs, sourceAndDestination.getSource(),
          sourceAndDestination.getDestination(), configuration));
    }
    return builders;
  }
  /**
   * Finds all files read by the Iceberg table including metadata json file, manifest files, nested manifest file paths and actual data files.
   * Returns a map of path, file status for each file that needs to be copied
   */
  protected Map<Path, FileStatus> getFilePaths() throws IOException {
    Map<Path, FileStatus> result = Maps.newHashMap();
    IcebergTable icebergTable = this.getIcebergTable();
    IcebergSnapshotInfo icebergSnapshotInfo = icebergTable.getCurrentSnapshotInfo();

    log.info("Fetching all file paths for the current snapshot of the Iceberg table");
    List<String> pathsToCopy = icebergSnapshotInfo.getAllPaths();

    for(String pathString : pathsToCopy) {
      Path path = new Path(pathString);
      result.put(path, this.fs.getFileStatus(path));
    }
    return result;
  }

  DatasetDescriptor getSourceDataset() {
    return getDatasetDescriptor(sourceMetastoreURI);
  }

  DatasetDescriptor getDestinationDataset() {
    return getDatasetDescriptor(targetMetastoreURI);
  }

  @NotNull
  private DatasetDescriptor getDatasetDescriptor(Optional<String> stringMetastoreURI) {
    String destinationTable = this.getDbName() + "." + this.getInputTableName();

    URI hiveMetastoreURI = null;
    if (stringMetastoreURI.isPresent()) {
      hiveMetastoreURI = URI.create(stringMetastoreURI.get());
    }

    DatasetDescriptor destinationDataset =
        new DatasetDescriptor(DatasetConstants.PLATFORM_ICEBERG, hiveMetastoreURI, destinationTable);
    destinationDataset.addMetadata(DatasetConstants.FS_URI, this.getFs().getUri().toString());
    return destinationDataset;
  }
}
