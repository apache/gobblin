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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.ConfigStoreFactory;


/**
 * An implementation of {@link ConfigStoreFactory} for creating {@link SimpleHDFSConfigStore}s. This class only works
 * the physical scheme {@link #HDFS_SCHEME_NAME}.
 *
 * @see SimpleHDFSConfigStore
 */
public class SimpleHDFSConfigStoreFactory implements ConfigStoreFactory<SimpleHDFSConfigStore> {

  protected static final String SIMPLE_HDFS_SCHEME_PREFIX = "simple-";
  protected static final String HDFS_SCHEME_NAME = "hdfs";

  /** Global namespace for properties if no scope is used */
  public static final String DEFAULT_CONFIG_NAMESPACE = SimpleHDFSConfigStoreFactory.class.getName();
  /** Scoped configuration properties */
  public static final String DEFAULT_STORE_URI_KEY = "default_store_uri";

  private final Optional<URI> defaultStoreURI;
  private final FileSystem defaultStoreFS;

  /** Instantiates a new instance using standard typesafe config defaults:
   * {@link ConfigFactory#load()} */
  public SimpleHDFSConfigStoreFactory() {
    this(ConfigFactory.load().getConfig(DEFAULT_CONFIG_NAMESPACE));
  }

  /**
   * Instantiates a new instance of the factory with the specified config. The configuration is
   * expected to be scoped, i.e. the properties should not be prefixed.
   */
  public SimpleHDFSConfigStoreFactory(Config factoryConfig) {
    try {
      if (factoryConfig.hasPath(DEFAULT_STORE_URI_KEY)) {
        String uri = factoryConfig.getString(DEFAULT_STORE_URI_KEY);
        if (Strings.isNullOrEmpty(uri)) {
          throw new IllegalArgumentException("Default store URI should be non-empty!");
        }
        Path defStorePath = new Path(uri);
        this.defaultStoreFS = defStorePath.getFileSystem(new Configuration());
        this.defaultStoreURI = Optional.of(this.defaultStoreFS.makeQualified(defStorePath).toUri());
        if (!isValidStoreRootPath(this.defaultStoreFS, defStorePath)) {
          throw new IllegalArgumentException("Path does not appear to be a config store root: " + this.defaultStoreFS);
        }
      } else {
        this.defaultStoreFS = FileSystem.get(new Configuration());
        Path candidateStorePath = getDefaultRootDir();
        if (isValidStoreRootPath(this.defaultStoreFS, candidateStorePath)) {
          this.defaultStoreURI = Optional.of(this.defaultStoreFS.makeQualified(candidateStorePath).toUri());
        } else {
          this.defaultStoreURI = Optional.absent();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize Hadoop FS store factory:" + e, e);
    }
  }

  private static boolean isValidStoreRootPath(FileSystem fs, Path storeRootPath) throws IOException {
    Path storeRoot = new Path(storeRootPath, SimpleHDFSConfigStore.CONFIG_STORE_NAME);
    return fs.exists(storeRoot);
  }

  @Override
  public String getScheme() {
    return SIMPLE_HDFS_SCHEME_PREFIX + getPhysicalScheme();
  }

  /**
   * Creates a {@link SimpleHDFSConfigStore} for the given {@link URI}. The {@link URI} specified should be the fully
   * qualified path to the dataset in question. For example,
   * {@code simple-hdfs://[authority]:[port][path-to-config-store][path-to-dataset]}. It is important to note that the
   * path to the config store on HDFS must also be specified. The combination
   * {@code [path-to-config-store][path-to-dataset]} need not specify an actual {@link Path} on HDFS.
   *
   * <p>
   *   If the {@link URI} does not contain an authority, a default authority and root directory are provided. The
   *   default authority is taken from the NameNode {@link URI} the current process is co-located with. The default path
   *   is "/user/[current-user]/".
   * </p>
   *
   * @param  configKey       The URI of the config key that needs to be accessed.
   *
   * @return a {@link SimpleHDFSConfigStore} configured with the the given {@link URI}.
   *
   * @throws ConfigStoreCreationException if the {@link SimpleHDFSConfigStore} could not be created.
   */
  @Override
  public SimpleHDFSConfigStore createConfigStore(URI configKey) throws ConfigStoreCreationException {
    FileSystem fs = createFileSystem(configKey);
    URI physicalStoreRoot = getStoreRoot(fs, configKey);
    URI logicalStoreRoot = URI.create(SIMPLE_HDFS_SCHEME_PREFIX + physicalStoreRoot);
    return new SimpleHDFSConfigStore(fs, physicalStoreRoot, logicalStoreRoot);
  }

  /**
   * Returns the physical scheme this {@link ConfigStoreFactory} is responsible for. To support new HDFS
   * {@link FileSystem} implementations, subclasses should override this method.
   */
  protected String getPhysicalScheme() {
    return this.defaultStoreFS.getUri().getScheme();
  }

  /**
   * Gets a default root directory if one is not specified. The default root dir is {@code /jobs/[current-user]/}.
   */
  protected Path getDefaultRootDir() throws IOException {
    return this.defaultStoreFS.getHomeDirectory();
  }

  /**
   * Gets a default authority if one is not specified. The default authority is the authority
   * configured as part of the {@link #DEFAULT_STORE_URI_KEY} configuration setting or the {@link FileSystem}
   * the current process is running if the setting is missing.. For example, when running on a
   * HDFS node, the authority will taken from the NameNode {@link URI}.
   */
  private String getDefaultAuthority() throws IOException {
    return this.defaultStoreFS.getUri().getAuthority();
  }

  /**
   * Creates a {@link FileSystem} given a user specified configKey.
   */
  private FileSystem createFileSystem(URI configKey) throws ConfigStoreCreationException {
    try {
      return FileSystem.get(createFileSystemURI(configKey), new Configuration());
    } catch (IOException | URISyntaxException e) {
      throw new ConfigStoreCreationException(configKey, e);
    }
  }

  /**
   * Creates a Hadoop FS {@link URI} given a user-specified configKey. If the given configKey does not have an authority,
   * a default one is used instead, provided by the {@link #getDefaultAuthority()} method.
   */
  private URI createFileSystemURI(URI configKey) throws URISyntaxException, IOException {
    // Validate the scheme
    String configKeyScheme = configKey.getScheme();
    if (!configKeyScheme.startsWith(SIMPLE_HDFS_SCHEME_PREFIX)) {
      throw new IllegalArgumentException(
          String.format("Scheme for configKey \"%s\" must begin with \"%s\"!", configKey, SIMPLE_HDFS_SCHEME_PREFIX));
    }

    if (Strings.isNullOrEmpty(configKey.getAuthority())) {
      return new URI(getPhysicalScheme(), getDefaultAuthority(), "", "", "");
    }
    String uriPhysicalScheme = configKeyScheme.substring(SIMPLE_HDFS_SCHEME_PREFIX.length(), configKeyScheme.length());
    return new URI(uriPhysicalScheme, configKey.getAuthority(), "", "", "");
  }

  /**
   * This method determines the physical location of the {@link SimpleHDFSConfigStore} root directory on HDFS. It does
   * this by taking the {@link URI} given by the user and back-tracing the path. It checks if each parent directory
   * contains the folder {@link SimpleHDFSConfigStore#CONFIG_STORE_NAME}. It the assumes this {@link Path} is the root
   * directory.
   *
   * <p>
   *   If the given configKey does not have an authority, then this method assumes the given {@link URI#getPath()} does
   *   not contain the dataset root. In which case it uses the {@link #getDefaultRootDir()} as the root directory. If
   *   the default root dir does not contain the {@link SimpleHDFSConfigStore#CONFIG_STORE_NAME} then a
   *   {@link ConfigStoreCreationException} is thrown.
   * </p>
   */
  private URI getStoreRoot(FileSystem fs, URI configKey) throws ConfigStoreCreationException {
    if (Strings.isNullOrEmpty(configKey.getAuthority())) {
      if (!hasDefaultStoreURI()) {
        throw new ConfigStoreCreationException(configKey, "No default store has been configured.");
      }
      return this.defaultStoreURI.get();
    }

    Path path = new Path(configKey.getPath());

    while (path != null) {
      try {
        // the abs URI may point to an unexist path for
        // 1. phantom node
        // 2. as URI did not specify the version
        if (fs.exists(path)) {
          for (FileStatus fileStatus : fs.listStatus(path)) {
            if (fileStatus.isDirectory()
                && fileStatus.getPath().getName().equals(SimpleHDFSConfigStore.CONFIG_STORE_NAME)) {
              return fs.getUri().resolve(fileStatus.getPath().getParent().toUri());
            }
          }
        }
      } catch (IOException e) {
        throw new ConfigStoreCreationException(configKey, e);
      }

      path = path.getParent();
    }
    throw new ConfigStoreCreationException(configKey, "Cannot find the store root!");
  }

  @VisibleForTesting
  boolean hasDefaultStoreURI() {
    return this.defaultStoreURI.isPresent();
  }

  @VisibleForTesting
  URI getDefaultStoreURI() {
    return this.defaultStoreURI.get();
  }
}
