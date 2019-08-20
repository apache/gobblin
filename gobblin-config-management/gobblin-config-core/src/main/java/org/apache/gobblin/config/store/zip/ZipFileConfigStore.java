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
package org.apache.gobblin.config.store.zip;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigStore;
import org.apache.gobblin.config.store.api.PhysicalPathNotExistException;
import org.apache.gobblin.config.store.api.VersionDoesNotExistException;
import org.apache.gobblin.config.store.hdfs.SimpleHadoopFilesystemConfigStore;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.sun.nio.zipfs.ZipFileSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;


/**
 * {@link ConfigStore} that uses a zipped file containing all the config store paths.
 *
 * Similar to {@link SimpleHadoopFilesystemConfigStore} but using java APIs instead of Hadoop APIs for the Filesystem to
 * allow reading the file without unzipping.
 *
 * It is assumed that the version passed in the constructor will be the only version used.
 */
@Slf4j
public class ZipFileConfigStore implements ConfigStore {

  private final FileSystem fs;
  private final URI logicalStoreRoot;
  private String version;
  private String storePrefix;

  /**
   * Construct a ZipFileConfigStore
   *
   * @param fs A {@link ZipFileSystem} created using the zip file or jar containing the config store
   * @param logicalStoreRoot URI of this config store's root
   * @param version Config store version to use (only version allowed for lookups is the version passed here)
   * @param storePrefix Prefix to use if all paths in config store are under a parent directory
   */
  public ZipFileConfigStore(ZipFileSystem fs, URI logicalStoreRoot, String version, String storePrefix) {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(logicalStoreRoot);
    Preconditions.checkNotNull(version);

    this.fs = fs;
    this.logicalStoreRoot = logicalStoreRoot;
    this.version = version;
    this.storePrefix = storePrefix;
  }

  @Override
  public String getCurrentVersion() {
    return this.version;
  }

  @Override
  public URI getStoreURI() {
    return this.logicalStoreRoot;
  }

  /**
   * Retrieves all the children of the given {@link ConfigKeyPath} using {@link Files#walk} to list files
   */
  @Override
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(version.equals(getCurrentVersion()));

    List<ConfigKeyPath> children = new ArrayList<>();
    Path datasetDir = getDatasetDirForKey(configKey);

    try {
      if (!Files.exists(this.fs.getPath(datasetDir.toString()))) {
        throw new PhysicalPathNotExistException(this.logicalStoreRoot,
            "Cannot find physical location:" + this.fs.getPath(datasetDir.toString()));
      }

      Stream<Path> files = Files.walk(datasetDir, 1);

      for (Iterator<Path> it = files.iterator(); it.hasNext();) {
        Path path = it.next();

        if (Files.isDirectory(path) && !path.equals(datasetDir)) {
          children.add(configKey.createChild(StringUtils.removeEnd(path.getName(path.getNameCount() - 1).toString(),
              SingleLinkedListConfigKeyPath.PATH_DELIMETER)));
        }

      }
      return children;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting children for configKey: \"%s\"", configKey), e);
    }
  }

  /**
   * Retrieves all the {@link ConfigKeyPath}s that are imported by the given {@link ConfigKeyPath}. Similar to
   * {@link SimpleHadoopFilesystemConfigStore#getOwnImports}
   */
  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version) {
    return getOwnImports(configKey, version, Optional.<Config>absent());
  }

  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version, Optional<Config> runtimeConfig)
      throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(version.equals(getCurrentVersion()));

    List<ConfigKeyPath> configKeyPaths = new ArrayList<>();
    Path datasetDir = getDatasetDirForKey(configKey);
    Path includesFile = this.fs.getPath(datasetDir.toString(), SimpleHadoopFilesystemConfigStore.INCLUDES_CONF_FILE_NAME);

    try {
      if (!Files.exists(includesFile)) {
        return configKeyPaths;
      }

      if (!Files.isDirectory(includesFile)) {
        try (InputStream includesConfInStream = Files.newInputStream(includesFile)) {
          configKeyPaths = SimpleHadoopFilesystemConfigStore.getResolvedConfigKeyPaths(includesConfInStream, runtimeConfig);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting config for configKey: \"%s\"", configKey), e);
    }

    return configKeyPaths;
  }

  /**
   * Retrieves the {@link Config} for the given {@link ConfigKeyPath}. Similar to
   * {@link SimpleHadoopFilesystemConfigStore#getOwnConfig}
   */
  @Override
  public Config getOwnConfig(ConfigKeyPath configKey, String version) throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(version.equals(getCurrentVersion()));

    Path datasetDir = getDatasetDirForKey(configKey);
    Path mainConfFile = this.fs.getPath(datasetDir.toString(), SimpleHadoopFilesystemConfigStore.MAIN_CONF_FILE_NAME);

    try {
      if (!Files.exists(mainConfFile)) {
        return ConfigFactory.empty();
      }

      if (!Files.isDirectory(mainConfFile)) {
        try (InputStream mainConfInputStream = Files.newInputStream(mainConfFile)) {
          return ConfigFactory.parseReader(new InputStreamReader(mainConfInputStream, Charsets.UTF_8));
        }
      }
      return ConfigFactory.empty();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting config for configKey: \"%s\"", configKey), e);
    }
  }

  private Path getDatasetDirForKey(ConfigKeyPath configKey) throws VersionDoesNotExistException {
    return this.fs.getPath(this.storePrefix, configKey.getAbsolutePathString());
  }
}
