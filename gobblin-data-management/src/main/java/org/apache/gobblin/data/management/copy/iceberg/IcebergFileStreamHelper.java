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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;

/**
 * File-based helper for Iceberg file streaming operations.
 *
 * This helper supports file streaming mode where Iceberg table files
 * are streamed as binary data without record-level processing.
 */
@Slf4j
public class IcebergFileStreamHelper implements TimestampAwareFileBasedHelper {

  private final State state;
  private final Configuration configuration;
  private FileSystem fileSystem;

  public IcebergFileStreamHelper(State state) {
    this.state = state;
    this.configuration = new Configuration();

    // Add any Hadoop configuration from job properties
    for (String key : state.getPropertyNames()) {
      if (key.startsWith("fs.") || key.startsWith("hadoop.")) {
        configuration.set(key, state.getProp(key));
      }
    }
  }

  @Override
  public void connect() throws FileBasedHelperException {
    try {
      this.fileSystem = FileSystem.get(configuration);
      log.info("Connected to Iceberg file stream helper with FileSystem: {}", fileSystem.getClass().getSimpleName());
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to initialize FileSystem for Iceberg file streaming", e);
    }
  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
    try {
      // For Iceberg, file discovery is handled by IcebergSource
      // This method returns files from work unit configuration
      List<String> filesToPull = state.getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "");
      log.debug("Returning {} files for processing", filesToPull.size());
      return filesToPull;
    } catch (Exception e) {
      throw new FileBasedHelperException("Failed to list files", e);
    }
  }

  @Override
  public InputStream getFileStream(String filePath) throws FileBasedHelperException {
    try {
      Path path = new Path(filePath);
      FileSystem fs = getFileSystemForPath(path);
      return fs.open(path);
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to get file stream for: " + filePath, e);
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      Path path = new Path(filePath);
      FileSystem fs = getFileSystemForPath(path);
      return fs.getFileStatus(path).getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to get file size for: " + filePath, e);
    }
  }

  @Override
  public long getFileMTime(String filePath) throws FileBasedHelperException {
    try {
      Path path = new Path(filePath);
      FileSystem fs = getFileSystemForPath(path);
      return fs.getFileStatus(path).getModificationTime();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to get file modification time for: " + filePath, e);
    }
  }

  /**
   * Get FileSystem for a given path, reusing the default FileSystem when possible.
   * 
   * <p>This method avoids creating unnecessary FileSystem instances by reusing the
   * default FileSystem for paths with the same scheme. Only creates a new FileSystem
   * if the path uses a different scheme (e.g., reading from HDFS while default is local).
   * 
   * @param path the path to get FileSystem for
   * @return FileSystem instance (either the default or a new one for cross-scheme access)
   * @throws IOException if FileSystem cannot be created
   */
  public FileSystem getFileSystemForPath(Path path) throws IOException {
    // If path has a different scheme than the default FileSystem, get scheme-specific FS
    if (path.toUri().getScheme() != null &&
      !path.toUri().getScheme().equals(fileSystem.getUri().getScheme())) {
      return path.getFileSystem(configuration);
    }
    return fileSystem;
  }

  @Override
  public void close() throws IOException {
    if (fileSystem != null) {
      try {
        fileSystem.close();
        log.info("Closed Iceberg file stream helper and FileSystem connection");
      } catch (IOException e) {
        log.warn("Error closing FileSystem connection", e);
        throw e;
      }
    } else {
      log.debug("Closing Iceberg file stream helper - no FileSystem to close");
    }
  }

}
