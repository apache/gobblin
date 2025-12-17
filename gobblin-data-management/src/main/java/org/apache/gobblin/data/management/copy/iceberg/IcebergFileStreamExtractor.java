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
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.source.extractor.filebased.FileBasedExtractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;


/**
 * Extractor for file streaming mode that creates FileAwareInputStream for each file.
 *
 * This extractor is used when {@code iceberg.record.processing.enabled=false} to stream
 * Iceberg table files as binary data to destinations like Azure, HDFS
 *
 * Each "record" is a {@link FileAwareInputStream} representing one file from
 * the Iceberg table. The downstream writer handles streaming the file content.
 */
@Slf4j
public class IcebergFileStreamExtractor extends FileBasedExtractor<String, FileAwareInputStream> {

  private final Map<String, String> fileToPartitionPathMap;
  private final Gson gson = new Gson();
  private final FileSystem targetFs;
  private final CopyConfiguration copyConfiguration;

  public IcebergFileStreamExtractor(WorkUnitState workUnitState) throws IOException {
    super(workUnitState, new IcebergFileStreamHelper(workUnitState));

    // Initialize target FileSystem and CopyConfiguration once
    String writerFsUri = workUnitState.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    Configuration writerConf = WriterUtils.getFsConfiguration(workUnitState);
    this.targetFs = FileSystem.get(URI.create(writerFsUri), writerConf);
    this.copyConfiguration = CopyConfiguration.builder(this.targetFs, workUnitState.getProperties()).build();

    // Load partition path mapping from work unit (set by IcebergSource)
    String partitionPathJson = workUnitState.getProp(IcebergSource.ICEBERG_FILE_PARTITION_PATH);
    if (!StringUtils.isBlank(partitionPathJson)) {
      try {
        this.fileToPartitionPathMap = gson.fromJson(partitionPathJson,
            new TypeToken<Map<String, String>>() {}.getType());
        log.info("Loaded partition path mapping for {} files", fileToPartitionPathMap.size());
      } catch (JsonSyntaxException e) {
        String errorMsg = String.format("Failed to parse partition path mapping from work unit. "
            + "Expected valid JSON map, got: '%s'. Error: %s",
            partitionPathJson.length() > 200 ? partitionPathJson.substring(0, 200) + "..." : partitionPathJson,
            e.getMessage());
        log.error(errorMsg, e);
        throw new IOException(errorMsg, e);
      }
    } else {
      this.fileToPartitionPathMap = Collections.emptyMap();
      log.info("No partition path mapping found in work unit");
    }
  }

  @Override
  public String getSchema() {
    // For file streaming, schema is not used by IdentityConverter; returning a constant
    return "FileAwareInputStream";
  }

  /**
   * Downloads a file and wraps it in a {@link FileAwareInputStream} for streaming to the destination.
   *
   * <p>This method performs the following operations:
   * <ol>
   *   <li>Opens an input stream for the source file using {@link IcebergFileStreamHelper}</li>
   *   <li>Retrieves source file metadata (FileStatus) from the source filesystem</li>
   *   <li>Computes the destination path, which may be partition-aware based on work unit metadata</li>
   *   <li>Builds a {@link CopyableFile} containing both source and destination metadata</li>
   *   <li>Wraps the input stream and metadata in a {@link FileAwareInputStream}</li>
   * </ol>
   *
   * @param filePath the absolute path to the source file to download
   * @return an iterator containing a single {@link FileAwareInputStream} wrapping the file
   * @throws IOException if the file cannot be opened, file metadata cannot be retrieved,
   *                     or destination path computation fails
   */
  @Override
  public Iterator<FileAwareInputStream> downloadFile(String filePath) throws IOException {
    log.info("Preparing FileAwareInputStream for file: {}", filePath);

    // Open source stream using fsHelper
    final InputStream inputStream;
    try {
      inputStream = this.getFsHelper().getFileStream(filePath);
    } catch (FileBasedHelperException e) {
      throw new IOException("Failed to open source stream for: " + filePath, e);
    }

    // Get source file metadata using fsHelper's FileSystem
    Path sourcePath = new Path(filePath);
    FileSystem originFs = ((IcebergFileStreamHelper) this.getFsHelper()).getFileSystemForPath(sourcePath);
    FileStatus originStatus = originFs.getFileStatus(sourcePath);

    // Compute partition-aware destination path
    String finalDir = this.workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);
    if (StringUtils.isBlank(finalDir)) {
      throw new IOException("Required configuration '" + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + "' is not set. "
          + "Cannot determine destination path for file: " + filePath);
    }
    Path destinationPath = computeDestinationPath(filePath, finalDir, sourcePath.getName());

    List<OwnerAndPermission> ancestorOwnerAndPermissionList =
        CopyableFile.resolveReplicatedOwnerAndPermissionsRecursively(originFs,
            sourcePath.getParent(), PathUtils.getRootPathChild(sourcePath), copyConfiguration);
    // Build CopyableFile using cached targetFs and copyConfiguration (initialized once in constructor)
    CopyableFile copyableFile = CopyableFile.fromOriginAndDestination(originFs, originStatus, destinationPath, this.copyConfiguration)
        .fileSet(workUnit.getProp(ConfigurationKeys.DATASET_URN_KEY))
        .datasetOutputPath(targetFs.getUri().getPath())
        .ancestorsOwnerAndPermission(ancestorOwnerAndPermissionList)
        .build();
    copyableFile.setFsDatasets(originFs, targetFs);

    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder()
        .file(copyableFile)
        .inputStream(inputStream)
        .split(Optional.absent())
        .build();

    return Collections.singletonList(fileAwareInputStream).iterator();
  }

  /**
   * Compute destination path with partition awareness.
   *
   * <p>If partition metadata is available for this file, the destination path will include
   * the partition path: {@code <finalDir>/<partitionPath>/<filename>}</p>
   *
   * <p>Otherwise, the file is placed directly under finalDir: {@code <finalDir>/<filename>}</p>
   *
   * @param sourceFilePath the source file path
   * @param finalDir the final directory from configuration
   * @param fileName the file name
   * @return the computed destination path
   */
  private Path computeDestinationPath(String sourceFilePath, String finalDir, String fileName) {
    String partitionPath = fileToPartitionPathMap.get(sourceFilePath);

    if (!StringUtils.isBlank(partitionPath)) {
      // Partition-aware path: <finalDir>/<partitionPath>/<filename>
      // Example: /data/table1/datepartition=2025-04-01/file.orc
      Path destinationPath = new Path(new Path(finalDir, partitionPath), fileName);
      log.info("Computed partition-aware destination: {} -> {}", sourceFilePath, destinationPath);
      return destinationPath;
    } else {
      // No partition info: <finalDir>/<filename>
      Path destinationPath = new Path(finalDir, fileName);
      log.info("Computed flat destination (no partition): {} -> {}", sourceFilePath, destinationPath);
      return destinationPath;
    }
  }

}
