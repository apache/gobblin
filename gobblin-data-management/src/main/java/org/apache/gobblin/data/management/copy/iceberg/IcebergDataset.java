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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.iceberg.TableMetadata;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.function.CheckedExceptionFunction;
import org.apache.gobblin.util.measurement.GrowthMilestoneTracker;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;

/**
 * Iceberg dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Getter
public class IcebergDataset implements PrioritizedCopyableDataset {
  private final IcebergTable srcIcebergTable;
  /* CAUTION: *hopefully* `destIcebergTable` exists... although that's not necessarily been verified yet */
  private final IcebergTable destIcebergTable;
  protected final Properties properties;
  protected final FileSystem sourceFs;
  protected final boolean shouldIncludeMetadataPath;
  private final boolean shouldTolerateMissingSourceFiles = true; // TODO: make parameterizable, if desired

  public IcebergDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable, Properties properties, FileSystem sourceFs, boolean shouldIncludeMetadataPath) {
    this.srcIcebergTable = srcIcebergTable;
    this.destIcebergTable = destIcebergTable;
    this.properties = properties;
    this.sourceFs = sourceFs;
    this.shouldIncludeMetadataPath = shouldIncludeMetadataPath;
  }

  @Override
  public String datasetURN() {
    return this.getFileSetId();
  }

  @Override
  public String getDatasetPath() {
    try {
      return this.destIcebergTable.accessTableMetadata().location();
    } catch (IcebergTable.TableNotFoundException e) {
      throw new RuntimeException(e);
    }
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

  /** @return unique ID for dataset (based on the source-side table), usable as a {@link CopyEntity#getFileSet}, for atomic publication grouping */
  protected String getFileSetId() {
    return this.srcIcebergTable.getTableId().toString();
  }

  /**
   * Generates {@link FileSet}s, being themselves able to generate {@link CopyEntity}s for all files, data and metadata,
   * comprising the iceberg/table, so as to fully specify remaining table replication.
   */
  protected Iterator<FileSet<CopyEntity>> createFileSets(FileSystem targetFs, CopyConfiguration configuration) {
    FileSet<CopyEntity> fileSet = new IcebergTableFileSet(this.getFileSetId(), this, targetFs, configuration);
    return Iterators.singletonIterator(fileSet);
  }

  /** Conveys {@link Path}-to-{@link FileStatus} mapping in combination with {@link TableMetadata} originating from the same atomic read of those paths */
  @Data
  @VisibleForTesting
  protected static final class GetFilePathsToFileStatusResult {
    private final Map<Path, FileStatus> pathsToFileStatus;
    private final TableMetadata tableMetadata;
  }

  /**
   * Finds all files, data and metadata, as {@link CopyEntity}s that comprise the table and fully specify remaining
   * table replication.
   */
  @VisibleForTesting
  Collection<CopyEntity> generateCopyEntities(FileSystem targetFs, CopyConfiguration copyConfig) throws IOException {
    String fileSet = this.getFileSetId();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    TableMetadata destTableMetadataBeforeSrcRead = getCurrentDestTableMetadata();
    GetFilePathsToFileStatusResult atomicGetPathsResult = getFilePathsToFileStatus(targetFs, copyConfig, this.shouldIncludeMetadataPath);
    Map<Path, FileStatus> pathToFileStatus = atomicGetPathsResult.getPathsToFileStatus();
    log.info("~{}~ found {} candidate source paths", fileSet, pathToFileStatus.size());

    Configuration defaultHadoopConfiguration = new Configuration();
    for (Map.Entry<Path, FileStatus> entry : pathToFileStatus.entrySet()) {
      Path srcPath = entry.getKey();
      FileStatus srcFileStatus = entry.getValue();
      // TODO: should be the same FS each time; try creating once, reusing thereafter, to not recreate wastefully
      FileSystem actualSourceFs = getSourceFileSystemFromFileStatus(srcFileStatus, defaultHadoopConfiguration);
      Path greatestAncestorPath = PathUtils.getRootPathChild(srcPath);

      // preserving ancestor permissions till root path's child between src and dest
      List<OwnerAndPermission> ancestorOwnerAndPermissionList =
          CopyableFile.resolveReplicatedOwnerAndPermissionsRecursively(actualSourceFs,
              srcPath.getParent(), greatestAncestorPath, copyConfig);
      CopyableFile fileEntity = CopyableFile.fromOriginAndDestination(
          actualSourceFs, srcFileStatus, targetFs.makeQualified(srcPath), copyConfig)
          .fileSet(fileSet)
          .datasetOutputPath(targetFs.getUri().getPath())
          .ancestorsOwnerAndPermission(ancestorOwnerAndPermissionList)
          .build();
      fileEntity.setSourceData(getSourceDataset(this.sourceFs));
      fileEntity.setDestinationData(getDestinationDataset(targetFs));
      copyEntities.add(fileEntity);
    }

    copyEntities.add(createPostPublishStep(atomicGetPathsResult.getTableMetadata(), destTableMetadataBeforeSrcRead));
    log.info("~{}~ generated {} copy entities", fileSet, copyEntities.size());
    return copyEntities;
  }

  /**
   * @return {@link TableMetadata} current at destination
   * @throws {@link IOException} if `this.destIcebergTable` does not exist
   */
  protected TableMetadata getCurrentDestTableMetadata() throws IOException {
    try {
      return destIcebergTable.accessTableMetadata();
    } catch (IcebergTable.TableNotFoundException tnfe) {
      log.error("No destination TableMetadata because table not found: '" + destIcebergTable.getTableId() + "'" , tnfe);
      throw new IOException(tnfe);
    }
  }

  /**
   * Finds all files of the Iceberg's current snapshot and also returns the {@link TableMetadata} atomically accessed while reading those paths
   * @param shouldIncludeMetadataPath whether to consider "metadata.json" (`getMetadataPath()`) as eligible for inclusion
   * @return combined result: both a map of path to file status for each file to copy plus the {@link TableMetadata}, atomically observed with those paths
   */
  protected GetFilePathsToFileStatusResult getFilePathsToFileStatus(FileSystem targetFs, CopyConfiguration copyConfig, boolean shouldIncludeMetadataPath)
      throws IOException {
    IcebergTable icebergTable = this.getSrcIcebergTable();
    /** @return whether `pathStr` is present on `targetFs`, caching results while tunneling checked exceptions outward */
    Function<String, Boolean> isPresentOnTarget = CheckedExceptionFunction.wrapToTunneled(pathStr ->
      // omit considering timestamp (or other markers of freshness), as files should be immutable
      // ATTENTION: `CopyContext.getFileStatus()`, to partake in caching
      copyConfig.getCopyContext().getFileStatus(targetFs, new Path(pathStr)).isPresent()
    );

    // check first for case of nothing to replicate, to avoid needless scanning of a potentially massive iceberg
    // NOTE: if `shouldIncludeMetadataPath` was false during the prior executions, this condition will be false
    IcebergSnapshotInfo currentSnapshotOverview = icebergTable.getCurrentSnapshotInfoOverviewOnly();
    if (currentSnapshotOverview.getMetadataPath().map(isPresentOnTarget).orElse(false) &&
        isPresentOnTarget.apply(currentSnapshotOverview.getManifestListPath())) {
      log.info("~{}~ skipping entire iceberg, since snapshot '{}' at '{}' and metadata '{}' both present on target",
          this.getFileSetId(), currentSnapshotOverview.getSnapshotId(),
          currentSnapshotOverview.getManifestListPath(),
          currentSnapshotOverview.getMetadataPath().orElse("<<ERROR: MISSING!>>"));
      TableMetadata readTimeTableMetadata = currentSnapshotOverview.getTableMetadata().orElseThrow(() -> new RuntimeException(
          String.format("~%s~ no table metadata for current snapshot '%s' at '%s' with metadata path '%s'",
              this.getFileSetId(), currentSnapshotOverview.getSnapshotId(),
              currentSnapshotOverview.getManifestListPath(),
              currentSnapshotOverview.getMetadataPath().orElse("<<ERROR: MISSING!>>"))));
      return new GetFilePathsToFileStatusResult(Maps.newHashMap(), readTimeTableMetadata);
    }

    List<TableMetadata> readTimeTableMetadataHolder = Lists.newArrayList(); // expecting exactly one elem
    Iterator<IcebergSnapshotInfo> icebergIncrementalSnapshotInfos = icebergTable.getIncrementalSnapshotInfosIterator();
    Iterator<String> filePathsIterator = Iterators.concat(
        Iterators.transform(icebergIncrementalSnapshotInfos, snapshotInfo -> {
          snapshotInfo.getTableMetadata().ifPresent(readTimeTableMetadataHolder::add);
          // log each snapshot, for context, in case of `FileNotFoundException` during `FileSystem.getFileStatus()`
          String manListPath = snapshotInfo.getManifestListPath();
          log.info("~{}~ loaded snapshot '{}' at '{}' from metadata path: '{}'", this.getFileSetId(),
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
          if (!isPresentOnTarget.apply(manListPath)) {
            List<String> missingPaths = snapshotInfo.getSnapshotApexPaths(shouldIncludeMetadataPath);
            for (IcebergSnapshotInfo.ManifestFileInfo mfi : snapshotInfo.getManifestFiles()) {
              if (!isPresentOnTarget.apply(mfi.getManifestFilePath())) {
                missingPaths.add(mfi.getManifestFilePath());
                // being incremental info, no listed paths would have appeared prior w/ other snapshots, so add all now.
                // skip verification despite corner case of a snapshot having reorganized/rebalanced manifest contents
                // during a period where replication fell so far behind that no snapshots listed among current metadata
                // are yet at dest.  since the consequence of unnecessary copy is merely wasted data transfer and
                // compute--and overall, potential is small--prefer sidestepping expense of exhaustive checking, since
                // file count may run into 100k+ (even beyond!)
                missingPaths.addAll(mfi.getListedFilePaths());
              }
            }
            log.info("~{}~ snapshot '{}': collected {} additional source paths",
                this.getFileSetId(), snapshotInfo.getSnapshotId(), missingPaths.size());
            return missingPaths.iterator();
          } else {
            log.info("~{}~ snapshot '{}' already present on target... skipping (including contents)",
                this.getFileSetId(), snapshotInfo.getSnapshotId());
            // IMPORTANT: separately consider metadata path, to handle case of 'metadata-only' snapshot reusing manifest list
            Optional<String> metadataPath = snapshotInfo.getMetadataPath();
            Optional<String> nonReplicatedMetadataPath = metadataPath.filter(p -> !isPresentOnTarget.apply(p));
            metadataPath.ifPresent(ignore ->
                log.info("~{}~ metadata IS {} present on target",
                    this.getFileSetId(),
                    !nonReplicatedMetadataPath.isPresent()
                        ? "already"
                        : shouldIncludeMetadataPath ? "NOT YET" : "NOT CHOSEN TO BE")
            );
            return nonReplicatedMetadataPath
                .filter(ignore -> shouldIncludeMetadataPath)
                .map(p -> Lists.newArrayList(p).iterator())
                .orElse(Collections.emptyIterator());
          }
        })
    );

    Map<Path, FileStatus> results = Maps.newHashMap();
    long numSourceFilesNotFound = 0L;
    Iterable<String> filePathsIterable = () -> filePathsIterator;
    try {
      // TODO: investigate whether streaming initialization of `Map` preferable--`getFileStatus()` network calls likely
      // to benefit from parallelism
      GrowthMilestoneTracker growthTracker = new GrowthMilestoneTracker();
      PathErrorConsolidator errorConsolidator = new PathErrorConsolidator();
      for (String pathString : filePathsIterable) {
        Path path = new Path(pathString);
        try {
          results.put(path, this.sourceFs.getFileStatus(path));
          if (growthTracker.isAnotherMilestone(results.size())) {
            log.info("~{}~ collected file status on '{}' source paths", this.getFileSetId(), results.size());
          }
        } catch (FileNotFoundException fnfe) {
          if (!shouldTolerateMissingSourceFiles) {
            throw fnfe;
          } else {
            // log, but otherwise swallow... to continue on
            String total = ++numSourceFilesNotFound + " total";
            String speculation = "either premature deletion broke time-travel or metadata read interleaved among delete";
            errorConsolidator.prepLogMsg(path).ifPresent(msg ->
                log.warn("~{}~ source {} ({}... {})", this.getFileSetId(), msg, speculation, total)
            );
          }
        }
      }
    } catch (CheckedExceptionFunction.WrappedIOException wrapper) {
      wrapper.rethrowWrapped();
    }

    if (readTimeTableMetadataHolder.size() != 1) {
      final int firstNumToShow = 5;
      String newline = System.lineSeparator();
      String errMsg = readTimeTableMetadataHolder.size() == 0
          ? String.format("~%s~ no table metadata ever encountered!", this.getFileSetId())
          : String.format("~%s~ multiple metadata (%d) encountered (exactly 1 expected) - first %d: [%s]",
              this.getFileSetId(), readTimeTableMetadataHolder.size(), firstNumToShow,
              readTimeTableMetadataHolder.stream().limit(firstNumToShow).map(md ->
                  md.uuid() + " - " + md.metadataFileLocation()
              ).collect(Collectors.joining(newline, newline, newline)));
      throw new RuntimeException(errMsg);
    }
    return new GetFilePathsToFileStatusResult(results, readTimeTableMetadataHolder.get(0));
  }

  /**
   * Stateful object to consolidate error messages (e.g. for logging), per a {@link Path} consolidation strategy.
   * OVERVIEW: to avoid run-away logging into the 1000s of lines, consolidate to parent (directory) level:
   * 1. on the first path within the dir, log that specific path
   * 2. on the second path within the dir, log the dir path as a summarization (with ellipsis)
   * 3. thereafter, skip, logging nothing
   * The directory, parent path is the default consolidation strategy, yet may be overridden.
   */
  @NotThreadSafe
  protected static class PathErrorConsolidator {
    private final Map<Path, Boolean> consolidatedPathToWhetherErrorLogged = Maps.newHashMap();

    /** @return consolidated message to log, iff appropriate; else `Optional.empty()` when deserves inhibition */
    public Optional<String> prepLogMsg(Path path) {
      Path consolidatedPath = calcPathConsolidation(path);
      Boolean hadAlreadyLoggedConsolidation = this.consolidatedPathToWhetherErrorLogged.get(consolidatedPath);
      if (!Boolean.valueOf(true).equals(hadAlreadyLoggedConsolidation)) {
        boolean shouldLogConsolidationNow = hadAlreadyLoggedConsolidation != null;
        consolidatedPathToWhetherErrorLogged.put(consolidatedPath, shouldLogConsolidationNow);
        String pathLogString = shouldLogConsolidationNow ? (consolidatedPath.toString() + "/...") : path.toString();
        return Optional.of("path" + (shouldLogConsolidationNow ? "s" : " ") + " not found: '" + pathLogString + "'");
      } else {
        return Optional.empty();
      }
    }

    /** @return a {@link Path} to consolidate around; default is: {@link Path#getParent()} */
    protected Path calcPathConsolidation(Path path) {
      return path.getParent();
    }
  }

  @VisibleForTesting
  static PathErrorConsolidator createPathErrorConsolidator() {
    return new PathErrorConsolidator();
  }

  /** Add layer of indirection to permit test mocking by working around `FileSystem.get()` `static` method */
  protected FileSystem getSourceFileSystemFromFileStatus(FileStatus fileStatus, Configuration hadoopConfig) throws IOException {
    return fileStatus.getPath().getFileSystem(hadoopConfig);
  }

  protected DatasetDescriptor getSourceDataset(FileSystem sourceFs) {
    return this.srcIcebergTable.getDatasetDescriptor(sourceFs);
  }

  protected DatasetDescriptor getDestinationDataset(FileSystem targetFs) {
    return this.destIcebergTable.getDatasetDescriptor(targetFs);
  }

  private PostPublishStep createPostPublishStep(TableMetadata readTimeSrcTableMetadata, TableMetadata justPriorDestTableMetadata) {
    // TODO: Filter properties specific to iceberg registration and avoid serializing every global property
    IcebergRegisterStep icebergRegisterStep = new IcebergRegisterStep(
        this.srcIcebergTable.getTableId(), this.destIcebergTable.getTableId(), readTimeSrcTableMetadata, justPriorDestTableMetadata, this.properties);
    return new PostPublishStep(getFileSetId(), Maps.newHashMap(), icebergRegisterStep, 0);
  }
}
