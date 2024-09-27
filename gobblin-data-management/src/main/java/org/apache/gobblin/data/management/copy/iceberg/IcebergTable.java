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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;

import static org.apache.gobblin.data.management.copy.iceberg.IcebergSnapshotInfo.ManifestFileInfo;


/**
 * Exposes metadata information for a single Iceberg table.
 */
@Slf4j
@AllArgsConstructor
public class IcebergTable {

  /** Indicate the table identified by `tableId` does not (or does no longer) exist in the catalog */
  public static class TableNotFoundException extends IOException {
    @Getter
    private final TableIdentifier tableId; // stored purely for logging / diagnostics

    public TableNotFoundException(TableIdentifier tableId) {
      super("Not found: '" + tableId + "'");
      this.tableId = tableId;
    }
  }

  @Getter
  private final TableIdentifier tableId;
  /** allow the {@link IcebergCatalog} creating this table to qualify its {@link DatasetDescriptor#getName()} used for lineage, etc. */
  private final String datasetDescriptorName;
  /** allow the {@link IcebergCatalog} creating this table to specify the {@link DatasetDescriptor#getPlatform()} used for lineage, etc. */
  private final String datasetDescriptorPlatform;
  private final TableOperations tableOps;
  private final String catalogUri;
  private final Table table;

  @VisibleForTesting
  IcebergTable(TableIdentifier tableId, TableOperations tableOps, String catalogUri) {
    this(tableId, tableId.toString(), DatasetConstants.PLATFORM_ICEBERG, tableOps, catalogUri, null);
  }

  @VisibleForTesting
  IcebergTable(TableIdentifier tableId, TableOperations tableOps, String catalogUri, Table table) {
    this(tableId, tableId.toString(), DatasetConstants.PLATFORM_ICEBERG, tableOps, catalogUri, table);
  }

  /** @return metadata info limited to the most recent (current) snapshot */
  public IcebergSnapshotInfo getCurrentSnapshotInfo() throws IOException {
    TableMetadata current = accessTableMetadata();
    return createSnapshotInfo(current.currentSnapshot(), Optional.of(current.metadataFileLocation()), Optional.of(current));
  }

  /** @return metadata info for most recent snapshot, wherein manifests and their child data files ARE NOT listed */
  public IcebergSnapshotInfo getCurrentSnapshotInfoOverviewOnly() throws IOException {
    TableMetadata current = accessTableMetadata();
    return createSnapshotInfo(current.currentSnapshot(), Optional.of(current.metadataFileLocation()), Optional.of(current), true);
  }

  /** @return metadata info for all known snapshots, ordered historically, with *most recent last* */
  public Iterator<IcebergSnapshotInfo> getAllSnapshotInfosIterator() throws IOException {
    TableMetadata current = accessTableMetadata();
    long currentSnapshotId = current.currentSnapshot().snapshotId();
    List<Snapshot> snapshots = current.snapshots();
    return Iterators.transform(snapshots.iterator(), snapshot -> {
      try {
        return IcebergTable.this.createSnapshotInfo(
            snapshot,
            currentSnapshotId == snapshot.snapshotId() ? Optional.of(current.metadataFileLocation()) : Optional.empty(),
            currentSnapshotId == snapshot.snapshotId() ? Optional.of(current) : Optional.empty()
        );
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * @return metadata info for all known snapshots, but incrementally, so overlapping entries within snapshots appear
   * only with the first as they're ordered historically, with *most recent last*.
   *
   * This means the {@link IcebergSnapshotInfo#getManifestFiles()} for the (n+1)-th element of the iterator will omit
   * all manifest files and listed data files, already reflected in a {@link IcebergSnapshotInfo#getManifestFiles()}
   * from the n-th or prior elements.  Given the order of the {@link Iterator<IcebergSnapshotInfo>} returned, this
   * mirrors the snapshot-to-file dependencies: each file is returned exactly once with the (oldest) snapshot from
   * which it first becomes reachable.
   *
   * Only the final {@link IcebergSnapshotInfo#getMetadataPath()} is present (for the snapshot it itself deems current).
   */
  public Iterator<IcebergSnapshotInfo> getIncrementalSnapshotInfosIterator() throws IOException {
    // TODO: investigate using `.addedFiles()`, `.deletedFiles()` to calc this
    Set<String> knownFilePaths = Sets.newHashSet(); // as absolute paths are clearly unique, use a single set for all
    return Iterators.filter(Iterators.transform(getAllSnapshotInfosIterator(), snapshotInfo -> {
      log.info("~{}~ before snapshot '{}' - '{}' total known iceberg paths",
          tableId, snapshotInfo.getSnapshotId(), knownFilePaths.size());
      if (false == knownFilePaths.add(snapshotInfo.getManifestListPath())) { // already known manifest list!
        return snapshotInfo.toBuilder().manifestListPath(null).build(); // use `null` as marker to surrounding `filter`
      }
      List<IcebergSnapshotInfo.ManifestFileInfo> novelManifestInfos = Lists.newArrayList();
      for (ManifestFileInfo mfi : snapshotInfo.getManifestFiles()) {
        if (true == knownFilePaths.add(mfi.getManifestFilePath())) { // heretofore unknown
          List<String> novelListedPaths = mfi.getListedFilePaths().stream()
              .filter(fpath -> true == knownFilePaths.add(fpath)) // heretofore unknown
              .collect(Collectors.toList());
          if (novelListedPaths.size() == mfi.getListedFilePaths().size()) { // nothing filtered
            novelManifestInfos.add(mfi); // reuse orig
          } else {
            novelManifestInfos.add(new ManifestFileInfo(mfi.getManifestFilePath(), novelListedPaths));
          }
        } // else, whenever recognized manifest file, skip w/ all its listed paths--which also all would be recognized
      }
      if (novelManifestInfos.size() == snapshotInfo.getManifestFiles().size()) { // nothing filtered
        return snapshotInfo; // reuse orig
      } else {
        return snapshotInfo.toBuilder().manifestFiles(novelManifestInfos).build(); // replace manifestFiles
      }
    }), snapshotInfo -> snapshotInfo.getManifestListPath() != null); // remove marked-as-repeat-manifest-list snapshots
  }

  /** @throws {@link IcebergTable.TableNotFoundException} when table does not exist */
  protected TableMetadata accessTableMetadata() throws TableNotFoundException {
    TableMetadata current = this.tableOps.current();
    return Optional.ofNullable(current).orElseThrow(() -> new TableNotFoundException(this.tableId));
  }

  protected IcebergSnapshotInfo createSnapshotInfo(Snapshot snapshot, Optional<String> metadataFileLocation, Optional<TableMetadata> currentTableMetadata)
      throws IOException {
    return createSnapshotInfo(snapshot, metadataFileLocation, currentTableMetadata, false);
  }

  protected IcebergSnapshotInfo createSnapshotInfo(Snapshot snapshot, Optional<String> metadataFileLocation, Optional<TableMetadata> currentTableMetadata,
      boolean skipManifestFileInfo) throws IOException {
    // TODO: verify correctness, even when handling 'delete manifests'!
    return new IcebergSnapshotInfo(
        snapshot.snapshotId(),
        Instant.ofEpochMilli(snapshot.timestampMillis()),
        metadataFileLocation,
        currentTableMetadata,
        snapshot.manifestListLocation(),
        // NOTE: unable to `.stream().map(m -> calcManifestFileInfo(m, tableOps.io()))` due to checked exception
        skipManifestFileInfo ? Lists.newArrayList() : calcAllManifestFileInfos(snapshot.allManifests(tableOps.io()), tableOps.io())
      );
  }

  protected static List<IcebergSnapshotInfo.ManifestFileInfo> calcAllManifestFileInfos(List<ManifestFile> manifests, FileIO io) throws IOException {
    List<ManifestFileInfo> result = Lists.newArrayList();
    for (ManifestFile manifest : manifests) {
      result.add(calcManifestFileInfo(manifest, io));
    }
    return result;
  }

  protected static IcebergSnapshotInfo.ManifestFileInfo calcManifestFileInfo(ManifestFile manifest, FileIO io) throws IOException {
    return new ManifestFileInfo(manifest.path(), discoverDataFilePaths(manifest, io));
  }

  protected static List<String> discoverDataFilePaths(ManifestFile manifest, FileIO io) throws IOException {
    try (CloseableIterable<String> manifestPathsIterable = ManifestFiles.readPaths(manifest, io)) {
      return Lists.newArrayList(manifestPathsIterable);
    }
  }

  public DatasetDescriptor getDatasetDescriptor(FileSystem fs) {
    DatasetDescriptor descriptor = new DatasetDescriptor(
        datasetDescriptorPlatform,
        URI.create(this.catalogUri),
        this.datasetDescriptorName
    );
    descriptor.addMetadata(DatasetConstants.FS_URI, fs.getUri().toString());
    return descriptor;
  }

  /** Registers {@link IcebergTable} after publishing data.
   * @param dstMetadata is null if destination {@link IcebergTable} is absent, in which case registration is skipped */
  protected void registerIcebergTable(TableMetadata srcMetadata, TableMetadata dstMetadata) {
    if (dstMetadata != null) {
      // Use current destination metadata as 'base metadata', but commit the source-side metadata
      // to synchronize source-side property deletion over to the destination
      this.tableOps.commit(dstMetadata, srcMetadata);
    }
  }

  /**
   * Retrieves a list of data files from the current snapshot that match the specified partition filter predicate.
   *
   * @param icebergPartitionFilterPredicate the predicate to filter partitions
   * @return a list of data files that match the partition filter predicate
   * @throws IOException if an I/O error occurs while accessing the table metadata or reading manifest files
   */
  public List<DataFile> getPartitionSpecificDataFiles(Predicate<StructLike> icebergPartitionFilterPredicate) throws IOException {
    TableMetadata tableMetadata = accessTableMetadata();
    Snapshot currentSnapshot = tableMetadata.currentSnapshot();
    log.info("Starting to copy data files from snapshot: {}", currentSnapshot.snapshotId());
    //TODO: Add support for deleteManifests as well later
    // Currently supporting dataManifests only
    List<ManifestFile> dataManifestFiles = currentSnapshot.dataManifests(this.tableOps.io());
    List<DataFile> dataFileList = new ArrayList<>();
    for (ManifestFile manifestFile : dataManifestFiles) {
      try (ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, this.tableOps.io());
          CloseableIterator<DataFile> dataFiles = manifestReader.iterator()) {
        dataFiles.forEachRemaining(dataFile -> {
          if (icebergPartitionFilterPredicate.test(dataFile.partition())) {
            dataFileList.add(dataFile.copy());
          }
        });
      } catch (IOException e) {
        log.warn("Failed to read manifest file: {} " , manifestFile.path(), e);
      }
    }
    log.info("Found {} data files to copy", dataFileList.size());
    return dataFileList;
  }

  /**
   * Overwrite partitions in the table with the specified list of data files.
   * <p>
   *   Overwrite partition replaces the partitions using the expression filter provided.
   * </p>
   * @param dataFiles the list of data files to replace partitions with
   * @param partitionColName the partition column name whose data files are to be replaced
   * @param partitionValue  the partition column value on which data files will be replaced
   */
  protected void overwritePartitions(List<DataFile> dataFiles, String partitionColName, String partitionValue) {
    if (dataFiles.isEmpty()) {
      return;
    }
    OverwriteFiles overwriteFiles = this.table.newOverwrite();
    overwriteFiles.overwriteByRowFilter(Expressions.equal(partitionColName, partitionValue));
    dataFiles.forEach(overwriteFiles::addFile);
    overwriteFiles.commit();
    this.tableOps.refresh();
  }

}
