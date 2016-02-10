package gobblin.config.store.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;

import org.apache.commons.io.IOUtils;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.util.PathUtils;


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
 *   {@code dataset1/child-dataset} using {@link ConfigClient#getConfig(URI)}, the specified {@link URI} should be
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
 * @see {@link SimpleHDFSConfigStoreFactory}
 */
public class SimpleHDFSConfigStore implements ConfigStore {

  protected static final String CONFIG_STORE_NAME = "_CONFIG_STORE";

  private static final String MAIN_CONF_FILE_NAME = "main.conf";
  private static final String INCLUDES_CONF_FILE_NAME = "includes.conf";

  private final FileSystem fs;
  private final URI physicalStoreRoot;
  private final URI logicalStoreRoot;
  private final Cache<String, Path> versions;

  /**
   * Constructs a {@link SimpleHDFSConfigStore} using a given {@link FileSystem} and a {@link URI} that points to the
   * physical location of the store root.
   *
   * @param fs the {@link FileSystem} the {@link ConfigStore} is stored on.
   * @param physicalStoreRoot the fully qualified {@link URI} of the physical store root, the {@link URI#getScheme()} of the
   *                          {@link URI} should match the {@link FileSystem#getScheme()} of the given {@link FileSystem}.
   * @param logicalStoreRoot the fully qualfied {@link URI} of the logical store root
   */
  protected SimpleHDFSConfigStore(FileSystem fs, URI physicalStoreRoot, URI logicalStoreRoot) {
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
  }

  /**
   * Returns a {@link String} representation of the highest version stored in the {@link ConfigStore}. This method
   * determines the highest version by doing an {@code ls} on the store root, and sorting the output based on the
   * {@link FileStatus#getPath()}.
   *
   * @return a {@link String} representing the current (highest) version of the {@link ConfigStore}.
   */
  @Override
  public String getCurrentVersion() {
    Path configStoreDir = new Path(new Path(this.physicalStoreRoot), CONFIG_STORE_NAME);

    try {
      if (!this.fs.exists(configStoreDir)) {
        throw new VersionDoesNotExistException(getStoreURI(), "CURRENT", "");
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Error while checking if the configStoreDir: \"%s\" exists", configStoreDir), e);
    }

    try {
      FileStatus[] fileStatuses = Iterables.toArray(
          Iterables.filter(Arrays.asList(this.fs.listStatus(configStoreDir)), new FileStatusIsDir()), FileStatus.class);
      Arrays.sort(fileStatuses, new FileStatusPathNameComparator());
      return fileStatuses[fileStatuses.length - 1].getPath().getName();
    } catch (IOException e) {
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
        if (fileStatus.isDir()) {
          children.add(configKey.createChild(fileStatus.getPath().getName()));
        }
      }
      return children;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting children for configKey: \"%s\"", configKey), e);
    }
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
  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version)
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
      if (!includesFileStatus.isDir()) {
        try (InputStream includesConfInStream = this.fs.open(includesFileStatus.getPath())) {
          configKeyPaths.addAll(Lists.newArrayList(
              Iterables.transform(IOUtils.readLines(includesConfInStream, Charsets.UTF_8), new IncludesToConfigKey())));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error while getting imports for configKey: \"%s\"", configKey), e);
    }
    return configKeyPaths;
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
  public Config getOwnConfig(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException {
    Preconditions.checkNotNull(configKey, "configKey cannot be null!");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "version cannot be null or empty!");

    Path datasetDir = getDatasetDirForKey(configKey, version);
    Path mainConfFile = new Path(datasetDir, MAIN_CONF_FILE_NAME);

    try {
      if (!this.fs.exists(mainConfFile)) {
        return ConfigFactory.empty();
      }

      FileStatus configFileStatus = this.fs.getFileStatus(mainConfFile);
      if (!configFileStatus.isDir()) {
        try (InputStream mainConfInputStream = this.fs.open(configFileStatus.getPath())) {
          return ConfigFactory.parseReader(new InputStreamReader(mainConfInputStream, Charsets.UTF_8));
        }
      } else {
        return ConfigFactory.empty();
      }
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
    return new Path(getVersionRoot(version), getDatasetFromConfigKey(configKey));
  }

  /**
   * Retrieves the name of a dataset from a given {@link ConfigKeyPath}, relative to the store root.
   */
  private String getDatasetFromConfigKey(ConfigKeyPath configKey) {
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
      Path versionRootPath = PathUtils.combinePaths(physicalStoreRoot.toString(), CONFIG_STORE_NAME, version);
      if (fs.isDirectory(versionRootPath)) {
        return versionRootPath;
      } else {
        throw new VersionDoesNotExistException(getStoreURI(), version,
            String.format("Cannot find specified version under root %s", versionRootPath));
      }
    }
  }

  /**
   * Implementation of {@link Comparator} that compares {@link FileStatus}es based on the path name.
   */
  public static class FileStatusPathNameComparator implements Comparator<FileStatus>, Serializable {

    @Override
    public int compare(FileStatus fileStatus1, FileStatus fileStatus2) {
      return fileStatus1.getPath().getName().compareTo(fileStatus2.getPath().getName());
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

  private static class FileStatusIsDir implements Predicate<FileStatus> {

    @Override
    public boolean apply(FileStatus input) {
      return input == null ? false : input.isDir();
    }
  }
}
