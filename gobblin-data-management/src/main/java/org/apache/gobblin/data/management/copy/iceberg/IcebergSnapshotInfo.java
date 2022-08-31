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

import com.google.common.collect.Lists;
import java.time.Instant;

import java.util.List;

import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Information about the metadata file and data file paths of a single Iceberg Snapshot.
 */
@Slf4j
@AllArgsConstructor
@Getter
public class IcebergSnapshotInfo {

  private final Long snapshotId;
  private final Instant timestamp;
  private final String metadataPath;
  private final String manifestListPath;
  private final List<String> manifestFilePaths;
  private final List<List<String>> manifestListedFilePaths;  // NOTE: order parallels that of `manifestFilePaths`

  public List<String> getAllPaths() {
    List<String> result = Lists.newArrayList(metadataPath, manifestListPath);
    result.addAll(manifestFilePaths);
    result.addAll(getAllDataFilePaths());
    return result;
  }

  public List<String> getAllDataFilePaths() {
    List<String> result = Lists.newArrayList(manifestListedFilePaths.stream().flatMap(List::stream).collect(Collectors.toList()));
    return result;
  }
}
