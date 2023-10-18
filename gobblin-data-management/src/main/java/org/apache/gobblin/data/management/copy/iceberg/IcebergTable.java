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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

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
  private final TableOperations tableOps;
  private final String catalogUri;

  /** @return metadata info limited to the most recent (current) snapshot */
  public IcebergSnapshotInfo getCurrentSnapshotInfo() throws IOException {
    TableMetadata current = accessTableMetadata();
    return createSnapshotInfo(current.currentSnapshot(), Optional.of(current.metadataFileLocation()));
  }

  /** @return metadata info for most recent snapshot, wherein manifests and their child data files ARE NOT listed */
  public IcebergSnapshotInfo getCurrentSnapshotInfoOverviewOnly() throws IOException {
    TableMetadata current = accessTableMetadata();
    return createSnapshotInfo(current.currentSnapshot(), Optional.of(current.metadataFileLocation()), true);
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
            currentSnapshotId == snapshot.snapshotId() ? Optional.of(current.metadataFileLocation()) : Optional.empty()
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

  protected IcebergSnapshotInfo createSnapshotInfo(Snapshot snapshot, Optional<String> metadataFileLocation) throws IOException {
    return createSnapshotInfo(snapshot, metadataFileLocation, false);
  }

  protected IcebergSnapshotInfo createSnapshotInfo(Snapshot snapshot, Optional<String> metadataFileLocation, boolean skipManifestFileInfo) throws IOException {
    // TODO: verify correctness, even when handling 'delete manifests'!
    return new IcebergSnapshotInfo(
        snapshot.snapshotId(),
        Instant.ofEpochMilli(snapshot.timestampMillis()),
        metadataFileLocation,
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
  protected DatasetDescriptor getDatasetDescriptor(FileSystem fs) {
    DatasetDescriptor descriptor = new DatasetDescriptor(
        DatasetConstants.PLATFORM_ICEBERG,
        URI.create(this.catalogUri),
        this.tableId.name()
    );
    descriptor.addMetadata(DatasetConstants.FS_URI, fs.getUri().toString());
    return descriptor;
  }

  /** Registers {@link IcebergTable} after publishing data.
   * @param dstMetadata is null if destination {@link IcebergTable} is absent, in which case registration is skipped */
  protected void registerIcebergTable(TableMetadata srcMetadata, TableMetadata dstMetadata) {
    if (dstMetadata != null) {
      // use current destination metadata as 'base metadata' and source as 'updated metadata' while committing
      this.tableOps.commit(dstMetadata, srcMetadata.replaceProperties(dstMetadata.properties()));
    }
  }
}
