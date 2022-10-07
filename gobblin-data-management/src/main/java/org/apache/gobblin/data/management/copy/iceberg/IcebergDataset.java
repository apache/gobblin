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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
  private final boolean shouldTolerateMissingSourceFiles = true; // TODO: make parameterizable, if desired

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
    Map<Path, FileStatus> pathToFileStatus = getFilePathsToFileStatus(targetFs, copyConfig);
    log.info("{}.{} - found {} candidate source paths", dbName, inputTableName, pathToFileStatus.size());

    Configuration defaultHadoopConfiguration = new Configuration();
    for (Map.Entry<Path, FileStatus> entry : pathToFileStatus.entrySet()) {
      Path srcPath = entry.getKey();
      FileStatus srcFileStatus = entry.getValue();
      // TODO: should be the same FS each time; try creating once, reusing thereafter, to not recreate wastefully
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

  /** Not intended to escape this class... yet `public` visibility in case it somehow does */
  @RequiredArgsConstructor
  public static class WrappedIOException extends RuntimeException {
    @Getter
    private final IOException wrappedException;

    public void rethrowWrapped() throws IOException {
      throw wrappedException;
    }
  }

  /**
   * Finds all files of the Iceberg's current snapshot
   * Returns a map of path, file status for each file that needs to be copied
   */
  protected Map<Path, FileStatus> getFilePathsToFileStatus(FileSystem targetFs, CopyConfiguration copyConfig) throws IOException {
    Map<Path, FileStatus> results = Maps.newHashMap();
    IcebergTable icebergTable = this.getIcebergTable();
    // check first for case of nothing to replicate, to avoid needless scanning of a potentially massive iceberg
    IcebergSnapshotInfo currentSnapshotOverview = icebergTable.getCurrentSnapshotInfoOverviewOnly();
    if (currentSnapshotOverview.getMetadataPath().map(p -> isPathPresentOnTarget(new Path(p), targetFs, copyConfig)).orElse(false) &&
        isPathPresentOnTarget(new Path(currentSnapshotOverview.getManifestListPath()), targetFs, copyConfig)) {
      log.info("{}.{} - skipping entire iceberg, since snapshot '{}' at '{}' and metadata '{}' both present on target",
          dbName, inputTableName, currentSnapshotOverview.getSnapshotId(),
          currentSnapshotOverview.getManifestListPath(),
          currentSnapshotOverview.getMetadataPath().orElse("<<ERROR: MISSING!>>"));
      return results;
    }
    Iterator<IcebergSnapshotInfo> icebergIncrementalSnapshotInfos = icebergTable.getIncrementalSnapshotInfosIterator();
    Iterator<String> filePathsIterator = Iterators.concat(
        Iterators.transform(icebergIncrementalSnapshotInfos, snapshotInfo -> {
          // log each snapshot, for context, in case of `FileNotFoundException` during `FileSystem.getFileStatus()`
          String manListPath = snapshotInfo.getManifestListPath();
          log.info("{}.{} - loaded snapshot '{}' at '{}' from metadata path: '{}'", dbName, inputTableName,
              snapshotInfo.getSnapshotId(), manListPath, snapshotInfo.getMetadataPath().orElse("<<inherited>>"));
          // ALGO: an iceberg's files form a tree of four levels: metadata.json -> manifest-list -> manifest -> data;
          // most critically, all are presumed immutable and uniquely named, although any may be replaced.  we depend
          // also on incremental copy being run always atomically: to commit each iceberg only upon its full success.
          // thus established, the presence of a file at dest (identified by path/name) guarantees its entire subtree is
          // already copied--and, given immutability, completion of a prior copy naturally renders that file up-to-date.
          // hence, its entire subtree may be short-circuited.  nevertheless, absence of a file at dest cannot imply
          // its entire subtree necessarily requires copying, because it is possible, even likely in practice, that some
          // metadata files would have been replaced (e.g. during snapshot compaction).  in such instances, at least
          // some of the children pointed to within could have been copied prior, when they previously appeared as a
          // child of the current file's predecessor (which this new meta file now replaces).
          if (!isPathPresentOnTarget(new Path(manListPath), targetFs, copyConfig)) {
            List<String> missingPaths = snapshotInfo.getSnapshotApexPaths();
            for (IcebergSnapshotInfo.ManifestFileInfo mfi : snapshotInfo.getManifestFiles()) {
              if (!isPathPresentOnTarget(new Path(mfi.getManifestFilePath()), targetFs, copyConfig)) {
                missingPaths.add(mfi.getManifestFilePath());
                mfi.getListedFilePaths().stream().filter(p ->
                    !isPathPresentOnTarget(new Path(p), targetFs, copyConfig)
                ).forEach(missingPaths::add);
              }
            }
            return missingPaths.iterator();
          } else {
            log.info("{}.{} - snapshot '{}' already present on target... skipping (with contents)",
                dbName, inputTableName, snapshotInfo.getSnapshotId());
            // IMPORTANT: separately consider metadata path, to handle case of 'metadata-only' snapshot reusing mf-list
            Optional<String> nonReplicatedMetadataPath = snapshotInfo.getMetadataPath().filter(p ->
                !isPathPresentOnTarget(new Path(p), targetFs, copyConfig));
            log.info("{}.{} - metadata is {}already present on target", dbName, inputTableName, nonReplicatedMetadataPath.isPresent() ? "NOT " : "");
            return nonReplicatedMetadataPath.map(p -> Lists.newArrayList(p).iterator()).orElse(Collections.emptyIterator());
          }
        })
    );
    Iterable<String> filePathsIterable = () -> filePathsIterator;
    try {
      // TODO: investigate whether streaming initialization of `Map` preferable--`getFileStatus()` network calls likely
      // to benefit from parallelism
      for (String pathString : filePathsIterable) {
        try {
          Path path = new Path(pathString);
          results.put(path, this.sourceFs.getFileStatus(path));
        } catch (FileNotFoundException fnfe) {
          if (!shouldTolerateMissingSourceFiles) {
            throw fnfe;
          } else {
            // log, but otherwise swallow... to continue on
            log.warn("MIA source file... did premature deletion subvert time-travel or maybe metadata read interleaved with delete?", fnfe);
          }
        }
      }
    } catch (WrappedIOException wrapper) {
      wrapper.rethrowWrapped();
    }
    return results;
  }

  /** @returns whether `path` is present on `targetFs`, tunneling checked exceptions and caching results throughout */
  protected static boolean isPathPresentOnTarget(Path path, FileSystem targetFs, CopyConfiguration copyConfig) {
    try {
      // omit considering timestamp (or other markers of freshness), as files should be immutable
      // ATTENTION: `CopyContext.getFileStatus()`, to partake in caching
      return copyConfig.getCopyContext().getFileStatus(targetFs, path).isPresent();
    } catch (IOException e) {
      throw new WrappedIOException(e); // halt execution and tunnel the original error outward
    }
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
