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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;

/**
 * Iceberg dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Getter
public class IcebergDataset implements PrioritizedCopyableDataset {
  private final String dbName;
  private final String inputTableName;
  private final IcebergTable icebergTable;
  protected final Properties properties;
  protected final FileSystem sourceFs;

  private final Optional<URI> sourceCatalogMetastoreURI;
  private final Optional<URI> targetCatalogMetastoreURI;

  /** Target metastore URI */
  public static final String TARGET_METASTORE_URI_KEY =
      IcebergDatasetFinder.ICEBERG_DATASET_PREFIX + ".copy.target.metastore.uri";
  /** Target database name */
  public static final String TARGET_DATABASE_KEY = IcebergDatasetFinder.ICEBERG_DATASET_PREFIX + ".copy.target.database";

  public IcebergDataset(String db, String table, IcebergTable icebergTbl, Properties properties, FileSystem sourceFs) {
    this.dbName = db;
    this.inputTableName = table;
    this.icebergTable = icebergTbl;
    this.properties = properties;
    this.sourceFs = sourceFs;
    this.sourceCatalogMetastoreURI = getAsOptionalURI(this.properties, IcebergDatasetFinder.ICEBERG_HIVE_CATALOG_METASTORE_URI_KEY);
    this.targetCatalogMetastoreURI = getAsOptionalURI(this.properties, TARGET_METASTORE_URI_KEY);
  }

  @Override
  public String datasetURN() {
    return this.getFileSetId();
  }

  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link #createFileSets}.
   */
  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration) {
    return createFileSets(targetFs, configuration);
  }
  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link #createFileSets}.
   */
  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration,
      Comparator<FileSet<CopyEntity>> prioritizer, PushDownRequestor<FileSet<CopyEntity>> requestor) {
    // TODO: Implement PushDownRequestor and priority based copy entity iteration
    return createFileSets(targetFs, configuration);
  }

  /** @return unique ID for this dataset, usable as a {@link CopyEntity}.fileset, for atomic publication grouping */
  protected String getFileSetId() {
    return this.dbName + "." + this.inputTableName;
  }

  /**
   * Generates {@link FileSet}s, being themselves able to generate {@link CopyEntity}s for all files, data and metadata,
   * comprising the iceberg/table, so as to fully specify remaining table replication.
   */
  protected Iterator<FileSet<CopyEntity>> createFileSets(FileSystem targetFs, CopyConfiguration configuration) {
    FileSet<CopyEntity> fileSet = new IcebergTableFileSet(this.getInputTableName(), this, targetFs, configuration);
    return Iterators.singletonIterator(fileSet);
  }

  /**
   * Finds all files, data and metadata, as {@link CopyEntity}s that comprise the table and fully specify remaining
   * table replication.
   */
  @VisibleForTesting
  Collection<CopyEntity> generateCopyEntities(FileSystem targetFs, CopyConfiguration copyConfig) throws IOException {
    String fileSet = this.getFileSetId();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    Map<Path, FileStatus> pathToFileStatus = getFilePathsToFileStatus();
    log.info("{}.{} - found {} candidate source paths", dbName, inputTableName, pathToFileStatus.size());

    Configuration defaultHadoopConfiguration = new Configuration();
    for (Map.Entry<Path, FileStatus> entry : pathToFileStatus.entrySet()) {
      Path srcPath = entry.getKey();
      FileStatus srcFileStatus = entry.getValue();
      // TODO: determine whether unnecessarily expensive to repeatedly re-create what should be the same FS: could it
      // instead be created once and reused thereafter?
      FileSystem actualSourceFs = getSourceFileSystemFromFileStatus(srcFileStatus, defaultHadoopConfiguration);

      // TODO: Add preservation of ancestor ownership and permissions!

      CopyableFile fileEntity = CopyableFile.fromOriginAndDestination(
          actualSourceFs, srcFileStatus, targetFs.makeQualified(srcPath), copyConfig)
          .fileSet(fileSet)
          .datasetOutputPath(targetFs.getUri().getPath())
          .build();
      fileEntity.setSourceData(getSourceDataset(this.sourceFs));
      fileEntity.setDestinationData(getDestinationDataset(targetFs));
      copyEntities.add(fileEntity);
    }
    log.info("{}.{} - generated {} copy entities", dbName, inputTableName, copyEntities.size());
    return copyEntities;
  }

  /**
   * Finds all files of the Iceberg's current snapshot
   * Returns a map of path, file status for each file that needs to be copied
   */
  protected Map<Path, FileStatus> getFilePathsToFileStatus() throws IOException {
    Map<Path, FileStatus> result = Maps.newHashMap();
    IcebergTable icebergTable = this.getIcebergTable();
    Iterator<IcebergSnapshotInfo> icebergIncrementalSnapshotInfos = icebergTable.getIncrementalSnapshotInfosIterator();
    Iterator<String> filePathsIterator = Iterators.concat(
        Iterators.transform(icebergIncrementalSnapshotInfos, snapshotInfo -> {
          // TODO: decide: is it too much to print for every snapshot--instead use `.debug`?
          log.info("{}.{} - loaded snapshot '{}' from metadata path: '{}'", dbName, inputTableName,
              snapshotInfo.getSnapshotId(), snapshotInfo.getMetadataPath().orElse("<<inherited>>"));
          return snapshotInfo.getAllPaths().iterator();
        })
    );
    Iterable<String> filePathsIterable = () -> filePathsIterator;
    // TODO: investigate whether streaming initialization of `Map` preferable--`getFileStatus` network calls would
    // likely benefit from parallelism
    for (String pathString : filePathsIterable) {
      Path path = new Path(pathString);
      result.put(path, this.sourceFs.getFileStatus(path));
    }
    return result;
  }

  /** Add layer of indirection to permit test mocking by working around `FileSystem.get()` `static` method */
  protected FileSystem getSourceFileSystemFromFileStatus(FileStatus fileStatus, Configuration hadoopConfig) throws IOException {
    return fileStatus.getPath().getFileSystem(hadoopConfig);
  }

  protected static Optional<URI> getAsOptionalURI(Properties props, String key) {
    return Optional.ofNullable(props.getProperty(key)).map(URI::create);
  }

  protected DatasetDescriptor getSourceDataset(FileSystem sourceFs) {
    return getDatasetDescriptor(sourceCatalogMetastoreURI, sourceFs);
  }

  protected DatasetDescriptor getDestinationDataset(FileSystem targetFs) {
    return getDatasetDescriptor(targetCatalogMetastoreURI, targetFs);
  }

  private DatasetDescriptor getDatasetDescriptor(Optional<URI> catalogMetastoreURI, FileSystem fs) {
    DatasetDescriptor descriptor = new DatasetDescriptor(
        DatasetConstants.PLATFORM_ICEBERG,
        catalogMetastoreURI.orElse(null),
        this.getFileSetId()
    );
    descriptor.addMetadata(DatasetConstants.FS_URI, fs.getUri().toString());
    return descriptor;
  }
}
