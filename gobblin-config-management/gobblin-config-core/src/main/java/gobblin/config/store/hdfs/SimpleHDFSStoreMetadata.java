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
package gobblin.config.store.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.deploy.FsDeploymentConfig;
import gobblin.util.HadoopUtils;


/**
 * A metadata accessor for an HDFS based {@link ConfigStore}. A HDFS based {@link ConfigStore} will have a file named
 * {@link #CONFIG_STORE_METADATA_FILENAME} that contains store metadata as key/value pairs. This class helps adding more
 * key/value pairs to the store metadata file and helps reading key/value pairs from the store metadata file. For
 * instance the current active version of the store is stored at {@link #CONFIG_STORE_METADATA_CURRENT_VERSION_KEY}.
 */
public class SimpleHDFSStoreMetadata {

  private static final String CONFIG_STORE_METADATA_FILENAME = "store-metadata.conf";
  private static final String CONFIG_STORE_METADATA_CURRENT_VERSION_KEY = "config.hdfs.store.version.current";

  private final FileSystem fs;
  private final Path storeMetadataFilePath;

  /**
   * Create a new {@link SimpleHDFSStoreMetadata} to read and write store metadata
   *
   * @param fs where metadata is stored
   * @param configStoreDir path to {@link SimpleHadoopFilesystemConfigStore#CONFIG_STORE_NAME}
   */
  SimpleHDFSStoreMetadata(final FileSystem fs, final Path configStoreDir) {
    this.storeMetadataFilePath = new Path(configStoreDir, CONFIG_STORE_METADATA_FILENAME);
    this.fs = fs;
  }

  /**
   * Writes the <code>config</code> to {@link #storeMetadataFilePath}. Creates a backup file at
   * <code>storeMetadataFilePath + ".bkp"</code> to recover old metadata in case of unexpected deployment failures
   *
   * @param config to be serialized
   * @throws IOException if there was any problem writing the <code>config</code> to the store metadata file.
   */
  void writeMetadata(Config config) throws IOException {

    Path storeMetadataFileBkpPath =
        new Path(this.storeMetadataFilePath.getParent(), this.storeMetadataFilePath.getName() + ".bkp");

    // Delete old backup file if exists
    HadoopUtils.deleteIfExists(this.fs, storeMetadataFileBkpPath, true);

    // Move current storeMetadataFile to backup
    if (this.fs.exists(this.storeMetadataFilePath)) {
      HadoopUtils.renamePath(this.fs, this.storeMetadataFilePath, storeMetadataFileBkpPath);
    }

    // Write new storeMetadataFile
    try (FSDataOutputStream outputStream =
        FileSystem.create(this.fs, this.storeMetadataFilePath, FsDeploymentConfig.DEFAULT_STORE_PERMISSIONS);) {
      outputStream.write(config.root().render(ConfigRenderOptions.concise()).getBytes(Charsets.UTF_8));
    } catch (Exception e) {
      // Restore from backup
      HadoopUtils.deleteIfExists(this.fs, this.storeMetadataFilePath, true);
      HadoopUtils.renamePath(this.fs, storeMetadataFileBkpPath, this.storeMetadataFilePath);
      throw new IOException(
          String.format("Failed to write store metadata at %s. Restored existing store metadata file from backup",
              this.storeMetadataFilePath),
          e);
    }
  }

  private void addMetadata(String key, String value) throws IOException {
    Config newConfig;
    if (isStoreMetadataFilePresent()) {
      newConfig = readMetadata().withValue(key, ConfigValueFactory.fromAnyRef(value));
    } else {
      newConfig = ConfigFactory.empty().withValue(key, ConfigValueFactory.fromAnyRef(value));
    }

    writeMetadata(newConfig);
  }

  /**
   * Update the current version of the store in {@link #CONFIG_STORE_METADATA_FILENAME} file at {@link #storeMetadataFilePath}
   *
   * @param version to be updated
   */
  void setCurrentVersion(String version) throws IOException {
    addMetadata(CONFIG_STORE_METADATA_CURRENT_VERSION_KEY, version);
  }

  /**
   * Get the current version from {@link #CONFIG_STORE_METADATA_FILENAME} file at {@link #storeMetadataFilePath}
   *
   */
  String getCurrentVersion() throws IOException {
    return readMetadata().getString(CONFIG_STORE_METADATA_CURRENT_VERSION_KEY);
  }

  /**
   * Get all metadata from the {@link #CONFIG_STORE_METADATA_FILENAME} file at {@link #storeMetadataFilePath}
   *
   */
  Config readMetadata() throws IOException {
    if (!isStoreMetadataFilePresent()) {
      throw new IOException("Store metadata file does not exist at " + this.storeMetadataFilePath);
    }
    try (InputStream storeMetadataInputStream = this.fs.open(this.storeMetadataFilePath)) {
      return ConfigFactory.parseReader(new InputStreamReader(storeMetadataInputStream, Charsets.UTF_8));
    }
  }

  private boolean isStoreMetadataFilePresent() throws IOException {
    return this.fs.exists(this.storeMetadataFilePath);
  }
}
