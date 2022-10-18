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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Data;

import com.google.common.collect.Lists;


/**
 * Information about the metadata file and data file paths of a single Iceberg Snapshot.
 */
@Builder(toBuilder = true)
@Data
public class IcebergSnapshotInfo {

  @Data
  public static class ManifestFileInfo {
    private final String manifestFilePath;
    private final List<String> listedFilePaths;
  }

  private final Long snapshotId;
  private final Instant timestamp;
  /** only for the current snapshot, being whom the metadata file 'belongs to'; `isEmpty()` for all other snapshots */
  private final Optional<String> metadataPath;
  private final String manifestListPath;
  private final List<ManifestFileInfo> manifestFiles;

  public List<String> getManifestFilePaths() {
    return manifestFiles.stream().map(ManifestFileInfo::getManifestFilePath).collect(Collectors.toList());
  }

  public List<String> getAllDataFilePaths() {
    return manifestFiles.stream().map(ManifestFileInfo::getListedFilePaths).flatMap(List::stream).collect(Collectors.toList());
  }

  /** @return the `manifestListPath` and `metadataPath`, if present */
  public List<String> getSnapshotApexPaths() {
    List<String> result = metadataPath.map(Lists::newArrayList).orElse(Lists.newArrayList());
    result.add(manifestListPath);
    return result;
  }

  public List<String> getAllPaths() {
    List<String> result = getSnapshotApexPaths();
    result.addAll(getManifestFilePaths());
    result.addAll(getAllDataFilePaths());
    return result;
  }
}
