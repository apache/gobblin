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

import java.util.List;
import java.util.stream.Collectors;
import java.io.IOException;
import java.time.Instant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Exposes metadata information for a single Iceberg table.
 */
@Slf4j
@AllArgsConstructor
public class IcebergTable {
  private final TableOperations tableOps;

  public IcebergSnapshotInfo getCurrentSnapshotInfo() throws IOException {
    TableMetadata current = tableOps.current();
    Snapshot snapshot = current.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests();
    return new IcebergSnapshotInfo(
        snapshot.snapshotId(),
        Instant.ofEpochMilli(snapshot.timestampMillis()),
        current.metadataFileLocation(),
        snapshot.manifestListLocation(),
        manifests.stream().map(ManifestFile::path).collect(Collectors.toList()),
        discoverAllDataFilePaths(manifests, tableOps.io())
      );
  }

  @VisibleForTesting
  static List<List<String>> discoverAllDataFilePaths(List<ManifestFile> manifests, FileIO io) throws IOException {
    List<List<String>> result = Lists.newArrayList();
    for (ManifestFile manifest : manifests) {
      result.add(discoverDataFilePaths(manifest, io));
    }
    return result;
  }

  @VisibleForTesting
  static List<String> discoverDataFilePaths(ManifestFile manifest, FileIO io) throws IOException {
    CloseableIterable<String> manifestPathsIterable = ManifestFiles.readPaths(manifest, io);
    try {
      return Lists.newArrayList(manifestPathsIterable);
    } finally {
      manifestPathsIterable.close();
    }
  }
}
