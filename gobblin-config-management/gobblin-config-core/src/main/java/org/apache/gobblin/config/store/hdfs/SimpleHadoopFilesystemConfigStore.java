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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreWithStableVersioning;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.config.store.deploy.ConfigStream;
import gobblin.config.store.deploy.Deployable;
import gobblin.config.store.deploy.DeployableConfigSource;
import gobblin.config.store.deploy.FsDeploymentConfig;
import gobblin.util.FileListUtils;
import gobblin.util.PathUtils;
import gobblin.util.io.SeekableFSInputStream;
import gobblin.util.io.StreamUtils;


/**
 * An implementation of {@link ConfigStore} backed by HDFS. The class assumes a simple file and directory layout
 * structure where each path under the root store directory corresponds to a dataset. The {@link #getStoreURI()} method
 * gives an {@link URI} that identifies the HDFS cluster being used, as well as the root directory of the store. When
 * querying this store, the scheme should be of the form {@code simple-[hdfs-scheme]} (a.k.a the logical scheme). For
 * example, if the store is located on a the local filesystem the scheme should be {@code simple-file}, if the store
 * is located on HDFS, the scheme should be {@code simple-hdfs}. This class can be constructed using a
 * {@link SimpleHDFSConfigStoreFactory}.
 *
 * <p>
 *   The class assumes a directory called {@link #CONFIG_STORE_NAME} is under the root directory. This folder should
 *   contain a directory for each version deployed to the {@link ConfigStore}. An example directory structure could look
 *   like: <br>
 *   <blockquote>
 *     <code>
 *       /root<br>
 *       &emsp;/my-simple-store<br>
 *       &emsp;&emsp;/_CONFIG_STORE<br>
 *       &emsp;&emsp;&emsp;/v1.0<br>
 *       &emsp;&emsp;&emsp;&emsp;/dataset1<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;/child-dataset<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;/main.conf<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;/includes.conf<br>
 *       &emsp;&emsp;&emsp;&emsp;/dataset2<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;/main.conf<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;/child-dataset<br>
 *       &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;/main.conf<br>
 *     </code>
 *   </blockquote>
 * </p>
 *
 * <p>
 *   In the above example, the root of the store is {@code /root/my-simple-store/}. The code automatically assumes that
 *   this folder contains a directory named {@link #CONFIG_STORE_NAME}. In order to access the dataset
 *   {@code dataset1/child-dataset} using ConfigClient#getConfig(URI), the specified {@link URI} should be
 *   {@code simple-hdfs://[authority]:[port]/root/my-simple-store/dataset1/child-dataset/}. Note this is the fully
 *   qualified path to the actual {@link #MAIN_CONF_FILE_NAME} file on HDFS, with the {@link #CONFIG_STORE_NAME} and the
 *   {@code version} directories removed.
 * </p>
 *
 * <p>
 *   All the {@link Config}s for a dataset should be put in the associated {@link #MAIN_CONF_FILE_NAME} file, and all
 *   the imports should be put in the associated {@link #INCLUDES_CONF_FILE_NAME} file.
 * </p>
 *
 * <p>
 *   This class is not responsible for deploying configurations from an external source to HDFS, only for reading them.
 * </p>
 *
 * @see SimpleHDFSConfigStoreFactory
 */
@Slf4j
@ConfigStoreWithStableVersioning
public class SimpleHadoopFilesystemConfigStore implements ConfigStore, Deployable<FsDeploymentConfig> {

  protected static final String CONFIG_STORE_NAME = "_CONFIG_STORE";

  private static final String MAIN_CONF_FILE_NAME = "main.conf";
  private static final String INCLUDES_CONF_FILE_NAME = "includes.conf";
  private static final String INCLUDES_KEY_NAME = "includes";

  private final FileSystem fs;
  private final URI physicalStoreRoot;
  private final URI logicalStoreRoot;
  private final Cache<String, Path> versions;
  private final SimpleHDFSStoreMetadata storeMetadata;

  /**
   * Constructs a {@link SimpleHadoopFilesystemConfigStore} using a given {@link FileSystem} and a {@link URI} that points to the
   * physical location of the store root.
   *
   * @param fs the {@link FileSystem} the {@link ConfigStore} is stored on.
   * @param physicalStoreRoot the fully qualified {@link URI} of the physical store root, the {@link URI#getScheme()} of the
   *                          {@link URI} should match the {@link FileSystem#getScheme()} of the given {@link FileSystem}.
   * @param logicalStoreRoot the fully qualfied {@link URI} of the logical store root
   */
  protected SimpleHadoopFilesystemConfigStore(FileSystem fs, URI physicalStoreRoot, URI logicalStoreRoot) {
    Preconditions.checkNotNull(fs, "fs cannot be null!");
    Preconditions.checkNotNull(physicalStoreRoot, "physicalStoreRoot cannot be null!");
    Preconditions.checkNotNull(logicalStoreRoot, "logicalStoreRoot cannot be null!");

    this.fs = fs;

    Preconditions.checkArgument(!Strings.isNullOrEmpty(physicalStoreRoot.getScheme()),
        "The physicalStoreRoot must have a valid scheme!");
    Preconditions.checkArgument(physicalStoreRoot.getScheme().equals(fs.getUri().getScheme()),
        "The scheme of the physicalStoreRoot and the filesystem must match!");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(physicalStoreRoot.getPath()),
        "The path of the physicalStoreRoot must be valid as it is the root of the store!");

    this.physicalStoreRoot = physicalStoreRoot;
    this.logicalStoreRoot = logicalStoreRoot;
    this.versions = CacheBuilder.newBuilder().build();
    this.storeMetadata = new SimpleHDFSStoreMetadata(fs, new Path(new Path(this.physicalStoreRoot), CONFIG_STORE_NAME));
  }

  /**
   * Returns a {@link String} representation of the active version stored in the {@link ConfigStore}. This method
   * determines the current active version by reading the {@link #CONFIG_STORE_METADATA_FILENAME} in
   * {@link #CONFIG_STORE_NAME}
   *
   * @return a {@link String} representing the current active version of the {@link ConfigStore}.
   */
  @Override
  public String getCurrentVersion() {
    try {
      return this.storeMetadata.getCurrentVersion();
    } catch (IOException e) {
      Path configStoreDir = new Path(new Path(this.physicalStoreRoot), CONFIG_STORE_NAME);
      throw new RuntimeException(
          String.format("Error while checking current version for configStoreDir: \"%s\"", configStoreDir), e);
    }
  }

  /**
   * Returns a {@link URI} representing the logical store {@link URI} where the {@link URI#getPath()} is the path to
   * the root of the {@link ConfigStore}.
   *
   * @return a {@link URI} representing the logical store {@link URI} (e.g. simple-hdfs://[authority]:[port][path-to-root]).
   */
  @Override
  public URI getStoreURI() {
    return this.logicalStoreRoot;
  }

  /**
   * Retrieves all the children of the given {@link ConfigKeyPath} by doing a {@code ls} on the {@link Path} specified
   * by the {@link ConfigKeyPath}. If the {@link Path} described by the {@link ConfigKeyPath} does not exist, an empty
   * {@link Collection} is returned.
   *
   * @param  configKey      the config key path whose children are necessary.
   * @param  version        specify the configuration version in the configuration store.
   *
   * @return a {@link Collection} of {@link ConfigKeyPath} where each entry is a child of the given configKey.
   *
   * @throws VersionDoesNotExistException if the version specified cannot be found in the {@link ConfigStore}.
   */
  @Override
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "version cannot be null or empty!");

    List<ConfigKeyPath> children = new ArrayList<>();
    Path datasetDir = getDatasetDirForKey(configKey, version);

    try {
      if (!this.fs.exists(datasetDir)) {
        return children;
      }

      for (FileStatus fileStatus : this.fs.listStatus(datasetDir)) {
        if (fileStatus.isDirectory()) {
          children.add(configKey.createChild(fileStatus.getPath().getName()));
        }
      }
      return children;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting children for configKey: \"%s\"", configKey), e);
    }
  }

  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version) {
    return getOwnImports(configKey, version, Optional.<Config>absent());
  }

  /**
   * Retrieves all the {@link ConfigKeyPath}s that are imported by the given {@link ConfigKeyPath}. This method does this
   * by reading the {@link #INCLUDES_CONF_FILE_NAME} file associated with the dataset specified by the given
   * {@link ConfigKeyPath}. If the {@link Path} described by the {@link ConfigKeyPath} does not exist, then an empty
   * {@link List} is returned.
   *
   * @param  configKey      the config key path whose tags are needed
   * @param  version        the configuration version in the configuration store.
   *
   * @return a {@link List} of {@link ConfigKeyPath}s where each entry is a {@link ConfigKeyPath} imported by the dataset
   * specified by the configKey.
   *
   * @throws VersionDoesNotExistException if the version specified cannot be found in the {@link ConfigStore}.
   */
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version, Optional<Config> runtimeConfig)
      throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "version cannot be null or empty!");

    List<ConfigKeyPath> configKeyPaths = new ArrayList<>();
    Path datasetDir = getDatasetDirForKey(configKey, version);
    Path includesFile = new Path(datasetDir, INCLUDES_CONF_FILE_NAME);

    try {
      if (!this.fs.exists(includesFile)) {
        return configKeyPaths;
      }

      FileStatus includesFileStatus = this.fs.getFileStatus(includesFile);
      if (!includesFileStatus.isDirectory()) {
        try (InputStream includesConfInStream = this.fs.open(includesFileStatus.getPath())) {
          /*
           * The includes returned are used to build a fallback chain.
           * With the natural order, if a key found in the first include it is not be overriden by the next include.
           * By reversing the list, the Typesafe fallbacks are constructed bottom up.
           */
          configKeyPaths.addAll(Lists.newArrayList(
              Iterables.transform(Lists.reverse(resolveIncludesList(IOUtils.readLines(includesConfInStream, Charsets.UTF_8), runtimeConfig)),
                  new IncludesToConfigKey())));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting config for configKey: \"%s\"", configKey), e);
    }

    return configKeyPaths;
  }

  /**
   * A helper to resolve System properties and Environment variables in includes paths
   * The method loads the list of unresolved <code>includes</code> into an in-memory {@link Config} object and reolves
   * with a fallback on {@link ConfigFactory#defaultOverrides()}
   *
   * @param includes list of unresolved includes
   * @return a list of resolved includes
   */
  @VisibleForTesting
  public static List<String> resolveIncludesList(List<String> includes, Optional<Config> runtimeConfig) {

    // Create a TypeSafe Config object with Key INCLUDES_KEY_NAME and value an array of includes
    StringBuilder includesBuilder = new StringBuilder();
    for (String include : includes) {
      // Skip comments
      if (StringUtils.isNotBlank(include) && !StringUtils.startsWith(include, "#")) {
        includesBuilder.append(INCLUDES_KEY_NAME).append("+=").append(include).append("\n");
      }
    }

    // Resolve defaultOverrides and environment variables.
    if (includesBuilder.length() > 0) {
      if (runtimeConfig.isPresent()) {
        return ConfigFactory.parseString(includesBuilder.toString()).withFallback(ConfigFactory.defaultOverrides())
            .withFallback(ConfigFactory.systemEnvironment()).withFallback(runtimeConfig.get()).resolve()
            .getStringList(INCLUDES_KEY_NAME);
      } else {
        return ConfigFactory.parseString(includesBuilder.toString()).withFallback(ConfigFactory.defaultOverrides())
            .withFallback(ConfigFactory.systemEnvironment()).resolve().getStringList(INCLUDES_KEY_NAME);
      }
    }

    return Collections.emptyList();
  }

  public static List<String> resolveIncludesList(List<String> includes) {
    return resolveIncludesList(includes, Optional.<Config>absent());
  }


  /**
   * Retrieves the {@link Config} for the given {@link ConfigKeyPath} by reading the {@link #MAIN_CONF_FILE_NAME}
   * associated with the dataset specified by the given {@link ConfigKeyPath}. If the {@link Path} described by the
   * {@link ConfigKeyPath} does not exist then an empty {@link Config} is returned.
   *
   * @param  configKey      the config key path whose properties are needed.
   * @param  version        the configuration version in the configuration store.
   *
   * @return a {@link Config} for the given configKey.
   *
   * @throws VersionDoesNotExistException if the version specified cannot be found in the {@link ConfigStore}.
   */
  @Override
  public Config getOwnConfig(ConfigKeyPath configKey, String version) throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "version cannot be null or empty!");

    Path datasetDir = getDatasetDirForKey(configKey, version);
    Path mainConfFile = new Path(datasetDir, MAIN_CONF_FILE_NAME);

    try {
      if (!this.fs.exists(mainConfFile)) {
        return ConfigFactory.empty();
      }

      FileStatus configFileStatus = this.fs.getFileStatus(mainConfFile);
      if (!configFileStatus.isDirectory()) {
        try (InputStream mainConfInputStream = this.fs.open(configFileStatus.getPath())) {
          return ConfigFactory.parseReader(new InputStreamReader(mainConfInputStream, Charsets.UTF_8));
        }
      }
      return ConfigFactory.empty();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting config for configKey: \"%s\"", configKey), e);
    }
  }

  /**
   * Retrieves the dataset dir on HDFS associated with the given {@link ConfigKeyPath} and the given version. This
   * directory contains the {@link #MAIN_CONF_FILE_NAME} and {@link #INCLUDES_CONF_FILE_NAME} file, as well as any child
   * datasets.
   */
  private Path getDatasetDirForKey(ConfigKeyPath configKey, String version) throws VersionDoesNotExistException {
    String datasetFromConfigKey = getDatasetFromConfigKey(configKey);

    if (StringUtils.isBlank(datasetFromConfigKey)) {
      return getVersionRoot(version);
    }

    return new Path(getVersionRoot(version), datasetFromConfigKey);
  }

  /**
   * Retrieves the name of a dataset from a given {@link ConfigKeyPath}, relative to the store root.
   */
  private static String getDatasetFromConfigKey(ConfigKeyPath configKey) {
    return StringUtils.removeStart(configKey.getAbsolutePathString(), SingleLinkedListConfigKeyPath.PATH_DELIMETER);
  }

  /**
   * Constructs a {@link Path} that points to the location of the given version of the {@link ConfigStore} on HDFS. If
   * this {@link Path} does not exist, a {@link VersionDoesNotExistException} is thrown.
   */
  private Path getVersionRoot(String version) throws VersionDoesNotExistException {

    try {
      return this.versions.get(version, new VersionRootLoader(version));
    } catch (ExecutionException e) {
      throw new RuntimeException(
          String.format("Error while checking if version \"%s\" for store \"%s\" exists", version, getStoreURI()), e);
    }
  }

  /**
   * Implementation of {@link Callable} that finds the root {@link Path} of a specified version. To be used in
   * conjunction with the {@link #versions} cache.
   */
  @AllArgsConstructor
  private class VersionRootLoader implements Callable<Path> {

    private String version;

    @Override
    public Path call() throws IOException {
      Path versionRootPath = PathUtils.combinePaths(SimpleHadoopFilesystemConfigStore.this.physicalStoreRoot.toString(),
          CONFIG_STORE_NAME, this.version);
      if (SimpleHadoopFilesystemConfigStore.this.fs.isDirectory(versionRootPath)) {
        return versionRootPath;
      }
      throw new VersionDoesNotExistException(getStoreURI(), this.version,
          String.format("Cannot find specified version under root %s", versionRootPath));
    }
  }

  /**
   * Implementation of {@link Function} that translates a {@link String} in an {@link #INCLUDES_CONF_FILE_NAME} file to
   * a {@link ConfigKeyPath}.
   */
  private static class IncludesToConfigKey implements Function<String, ConfigKeyPath> {

    @Override
    public ConfigKeyPath apply(String input) {
      if (input == null) {
        return null;
      }
      ConfigKeyPath configKey = SingleLinkedListConfigKeyPath.ROOT;
      for (String file : Splitter.on(SingleLinkedListConfigKeyPath.PATH_DELIMETER).omitEmptyStrings().split(input)) {
        configKey = configKey.createChild(file);
      }

      return configKey;
    }
  }

  /**
   * Deploy configs provided by {@link FsDeploymentConfig#getDeployableConfigSource()} to HDFS.
   * For each {@link ConfigStream} returned by {@link DeployableConfigSource#getConfigStreams()}, creates a resource on HDFS.
   * <br>
   * <ul> Does the following:
   * <li> Read {@link ConfigStream}s and write them to HDFS
   * <li> Create parent directories of {@link ConfigStream#getConfigPath()} if required
   * <li> Set {@link FsDeploymentConfig#getStorePermissions()} to all resourced created on HDFS
   * <li> Update current active version in the store metadata file.
   * </ul>
   *
   * <p>
   *  For example: If "test-root" is a resource in classpath and all resources under it needs to be deployed,
   * <br>
   * <br>
   * <b>In Classpath:</b><br>
   * <blockquote> <code>
   *       test-root<br>
   *       &emsp;/data<br>
   *       &emsp;&emsp;/set1<br>
   *       &emsp;&emsp;&emsp;/main.conf<br>
   *       &emsp;/tag<br>
   *       &emsp;&emsp;/tag1<br>
   *       &emsp;&emsp;&emsp;/main.conf<br>
   *     </code> </blockquote>
   * </p>
   *
   * <p>
   *  A new version 2.0.0 {@link FsDeploymentConfig#getNewVersion()} is created on HDFS under <code>this.physicalStoreRoot/_CONFIG_STORE</code>
   * <br>
   * <br>
   * <b>On HDFS after deploy:</b><br>
   * <blockquote> <code>
   *       /_CONFIG_STORE<br>
   *       &emsp;/2.0.0<br>
   *       &emsp;&emsp;/data<br>
   *       &emsp;&emsp;&emsp;/set1<br>
   *       &emsp;&emsp;&emsp;&emsp;/main.conf<br>
   *       &emsp;&emsp;/tag<br>
   *       &emsp;&emsp;&emsp;/tag1<br>
   *       &emsp;&emsp;&emsp;&emsp;/main.conf<br>
   *     </code> </blockquote>
   * </p>
   *
   */
  @Override
  public void deploy(FsDeploymentConfig deploymentConfig) throws IOException {

    log.info("Deploying with config : " + deploymentConfig);

    Path hdfsconfigStoreRoot = new Path(this.physicalStoreRoot.getPath(), CONFIG_STORE_NAME);

    if (!this.fs.exists(hdfsconfigStoreRoot)) {
      throw new IOException("Config store root not present at " + this.physicalStoreRoot.getPath());
    }

    Path hdfsNewVersionPath = new Path(hdfsconfigStoreRoot, deploymentConfig.getNewVersion());

    if (!this.fs.exists(hdfsNewVersionPath)) {

      this.fs.mkdirs(hdfsNewVersionPath, deploymentConfig.getStorePermissions());

      Set<ConfigStream> confStreams = deploymentConfig.getDeployableConfigSource().getConfigStreams();

      for (ConfigStream confStream : confStreams) {
        String confAtPath = confStream.getConfigPath();

        log.info("Copying resource at : " + confAtPath);

        Path hdsfConfPath = new Path(hdfsNewVersionPath, confAtPath);

        if (!this.fs.exists(hdsfConfPath.getParent())) {
          this.fs.mkdirs(hdsfConfPath.getParent());
        }

        // If an empty directory needs to created it may not have a stream.
        if (confStream.getInputStream().isPresent()) {
          // Read the resource as a stream from the classpath and write it to HDFS
          try (SeekableFSInputStream inputStream = new SeekableFSInputStream(confStream.getInputStream().get());
              FSDataOutputStream os = this.fs.create(hdsfConfPath, false)) {
            StreamUtils.copy(inputStream, os);
          }
        }
      }

      // Set permission for newly copied files
      for (FileStatus fileStatus : FileListUtils.listPathsRecursively(this.fs, hdfsNewVersionPath,
          FileListUtils.NO_OP_PATH_FILTER)) {
        this.fs.setPermission(fileStatus.getPath(), deploymentConfig.getStorePermissions());
      }

    } else {
      log.warn(String.format(
          "STORE WITH VERSION %s ALREADY EXISTS. NEW RESOURCES WILL NOT BE COPIED. ONLY STORE MEATADATA FILE WILL BE UPDATED TO %s",
          deploymentConfig.getNewVersion(), deploymentConfig.getNewVersion()));
    }

    this.storeMetadata.setCurrentVersion(deploymentConfig.getNewVersion());

    log.info(String.format("New version %s of config store deployed at %s", deploymentConfig.getNewVersion(),
        hdfsconfigStoreRoot));
  }

  @VisibleForTesting
  URI getPhysicalStoreRoot() {
    return this.physicalStoreRoot;
  }
}
