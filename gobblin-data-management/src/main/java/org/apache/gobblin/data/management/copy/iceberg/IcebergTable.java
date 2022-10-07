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
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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

  private final TableIdentifier tableId;
  private final TableOperations tableOps;

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
   * @return metadata info for all known snapshots, but incrementally, so content overlap between snapshots appears
   * only within the first as they're ordered historically, with *most recent last*
   */
  public Iterator<IcebergSnapshotInfo> getIncrementalSnapshotInfosIterator() throws IOException {
    // TODO: investigate using `.addedFiles()`, `.deletedFiles()` to calc this
    Set<String> knownManifestListFilePaths = Sets.newHashSet();
    Set<String> knownManifestFilePaths = Sets.newHashSet();
    Set<String> knownListedFilePaths = Sets.newHashSet();
    return Iterators.filter(Iterators.transform(getAllSnapshotInfosIterator(), snapshotInfo -> {
      if (false == knownManifestListFilePaths.add(snapshotInfo.getManifestListPath())) { // already known manifest list!
        return snapshotInfo.toBuilder().manifestListPath(null).build(); // use `null` as marker to surrounding `filter`
      }
      List<IcebergSnapshotInfo.ManifestFileInfo> novelManifestInfos = Lists.newArrayList();
      for (ManifestFileInfo mfi : snapshotInfo.getManifestFiles()) {
        if (true == knownManifestFilePaths.add(mfi.getManifestFilePath())) { // heretofore unknown
          List<String> novelListedPaths = mfi.getListedFilePaths().stream()
              .filter(fpath -> true == knownListedFilePaths.add(fpath)) // heretofore unknown
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
        skipManifestFileInfo ? Lists.newArrayList() : calcAllManifestFileInfos(snapshot.allManifests(), tableOps.io())
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
    CloseableIterable<String> manifestPathsIterable = ManifestFiles.readPaths(manifest, io);
    try {
      return Lists.newArrayList(manifestPathsIterable);
    } finally {
      manifestPathsIterable.close();
    }
  }
}
