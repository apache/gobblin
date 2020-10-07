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

package org.apache.gobblin.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import org.apache.gobblin.configuration.ConfigurationKeys;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Used to load pull files from the file system.
 */
@Slf4j
@Getter
public class PullFileLoader {

  public static final String GLOBAL_PROPS_EXTENSION = ".properties";
  public static final PathFilter GLOBAL_PROPS_PATH_FILTER = new ExtensionFilter(GLOBAL_PROPS_EXTENSION);

  public static final String GLOBAL_HOCON_EXTENSION = ".configuration";
  public static final PathFilter GLOBAL_HOCON_PATH_FILTER = new ExtensionFilter(GLOBAL_HOCON_EXTENSION);

  public static final PathFilter GLOBAL_PATH_FILTER =
      new ExtensionFilter(Lists.newArrayList(GLOBAL_PROPS_EXTENSION, GLOBAL_HOCON_EXTENSION));

  public static final Set<String> DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS = Sets.newHashSet("pull", "job");
  public static final Set<String> DEFAULT_HOCON_PULL_FILE_EXTENSIONS = Sets.newHashSet("json", "conf");

  public static final String PROPERTY_DELIMITER_PARSING_ENABLED_KEY = "property.parsing.enablekey";
  public static final boolean DEFAULT_PROPERTY_DELIMITER_PARSING_ENABLED_KEY = false;

  private final Path rootDirectory;
  private final FileSystem fs;
  private final ExtensionFilter javaPropsPullFileFilter;
  private final ExtensionFilter hoconPullFileFilter;

  /**
   * A {@link PathFilter} that accepts {@link Path}s based on a set of valid extensions.
   */
  private static class ExtensionFilter implements PathFilter {
    private final Collection<String> extensions;

    public ExtensionFilter(String extension) {
      this(Lists.newArrayList(extension));
    }

    public ExtensionFilter(Collection<String> extensions) {
      this.extensions = Lists.newArrayList();
      for (String ext : extensions) {
        this.extensions.add(ext.startsWith(".") ? ext : "." + ext);
      }
    }

    @Override
    public boolean accept(final Path path) {
      Predicate<String> predicate = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
          return path.getName().toLowerCase().endsWith(input);
        }
      };
      return Iterables.any(this.extensions, predicate);
    }
  }

  public PullFileLoader(Path rootDirectory, FileSystem fs, Collection<String> javaPropsPullFileExtensions,
      Collection<String> hoconPullFileExtensions) {

    Set<String> commonExtensions = Sets.intersection(Sets.newHashSet(javaPropsPullFileExtensions), Sets.newHashSet(hoconPullFileExtensions));
    Preconditions.checkArgument(commonExtensions.isEmpty(),
        "Java props and HOCON pull file extensions intersect: " + Arrays.toString(commonExtensions.toArray()));

    this.rootDirectory = rootDirectory;
    this.fs = fs;
    this.javaPropsPullFileFilter = new ExtensionFilter(javaPropsPullFileExtensions);
    this.hoconPullFileFilter = new ExtensionFilter(hoconPullFileExtensions);
  }

  /**
   * Load a single pull file.
   * @param path The {@link Path} to the pull file to load, full path
   * @param sysProps A {@link Config} used as fallback.
   * @param loadGlobalProperties if true, will also load at most one *.properties file per directory from the
   *          {@link #rootDirectory} to the pull file {@link Path}.
   * @param resolve if true, call {@link Config#resolve()} on the config after loading it
   * @return The loaded {@link Config}.
   * @throws IOException
   */
  public Config loadPullFile(Path path, Config sysProps, boolean loadGlobalProperties, boolean resolve)
      throws IOException {
    Config fallback = loadGlobalProperties ? loadAncestorGlobalConfigs(path, sysProps) : sysProps;
    Config loadedConfig;
    if (this.javaPropsPullFileFilter.accept(path)) {
      loadedConfig = loadJavaPropsWithFallback(path, fallback);
    } else if (this.hoconPullFileFilter.accept(path)) {
      loadedConfig = loadHoconConfigAtPath(path).withFallback(fallback);
    } else {
      throw new IOException(String.format("Cannot load pull file %s due to unrecognized extension.", path));
    }

    return resolve ? loadedConfig.resolve() : loadedConfig;
  }

  public Config loadPullFile(Path path, Config sysProps, boolean loadGlobalProperties)
      throws IOException {
    return loadPullFile(path, sysProps, loadGlobalProperties, true);
  }

  /**
   * Find and load all pull files under a base {@link Path} recursively in an order sorted by last modified date.
   * @param path base {@link Path} where pull files should be found recursively.
   * @param sysProps A {@link Config} used as fallback.
   * @param loadGlobalProperties if true, will also load at most one *.properties file per directory from the
   *          {@link #rootDirectory} to the pull file {@link Path} for each pull file.
   * @return The loaded {@link Config}s. Files that fail to parse successfully will be logged,
   *         but will not result in a Config object
   *
   */
  public List<Config> loadPullFilesRecursively(Path path, Config sysProps, boolean loadGlobalProperties) {
    return Lists.transform(this.fetchJobFilesRecursively(path), new Function<Path, Config>() {
      @Nullable
      @Override
      public Config apply(@Nullable Path jobFile) {
        if (jobFile == null) {
          return null;
        }

        try {
          return PullFileLoader.this.loadPullFile(jobFile, sysProps, loadGlobalProperties);
        } catch (IOException ie) {
          log.error("", ie);
          return null;
        }
      }
    }).stream().filter(Objects::nonNull).collect(Collectors.toList());
    // only return valid parsed configs
  }

  public List<Path> fetchJobFilesRecursively(Path path) {
    return getSortedPaths(fetchJobFilePathsRecursivelyHelper(path));
  }

  private List<Path> getSortedPaths(List<PathWithTimeStamp> pathsWithTimeStamps) {
    List<Path> sortedPaths = Lists.newArrayList();
    Collections.sort(pathsWithTimeStamps, Comparator.comparingLong(o -> o.timeStamp));
    for (PathWithTimeStamp pathWithTimeStamp : pathsWithTimeStamps) {
      sortedPaths.add(pathWithTimeStamp.path);
    }
    return sortedPaths;
  }

  private List<PathWithTimeStamp> fetchJobFilePathsRecursivelyHelper(Path path) {
    List<PathWithTimeStamp> paths = Lists.newArrayList();
    try {
      FileStatus[] statuses = this.fs.listStatus(path);
      if (statuses == null) {
        log.error("Path does not exist: " + path);
        return paths;
      }

      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          paths.addAll(fetchJobFilePathsRecursivelyHelper(status.getPath()));
        } else if (this.javaPropsPullFileFilter.accept(status.getPath())) {
          log.debug("modification time of {} is {}", status.getPath(), status.getModificationTime());
          paths.add(new PathWithTimeStamp(status.getModificationTime(), status.getPath()));
        } else if (this.hoconPullFileFilter.accept(status.getPath())) {
          log.debug("modification time of {} is {}", status.getPath(), status.getModificationTime());
          paths.add(new PathWithTimeStamp(status.getModificationTime(), status.getPath()));
        }
      }

      return paths;
    } catch (IOException ioe) {
      log.error("Could not load properties at path: " + path, ioe);
      return Lists.newArrayList();
    }
  }

  /**
   * Load at most one *.properties files from path and each ancestor of path up to and including {@link #rootDirectory}.
   * Higher directories will serve as fallback for lower directories, and sysProps will serve as fallback for all of them.
   * @throws IOException
   */
  private Config loadAncestorGlobalConfigs(Path path, Config sysProps)
      throws IOException {
    Config config = sysProps;

    if (!PathUtils.isAncestor(this.rootDirectory, path)) {
      log.warn(String.format("Loaded path %s is not a descendant of root path %s. Cannot load global properties.", path,
          this.rootDirectory));
    } else {

      List<Path> ancestorPaths = Lists.newArrayList();
      while (PathUtils.isAncestor(this.rootDirectory, path)) {
        ancestorPaths.add(path);
        path = path.getParent();
      }

      List<Path> reversedAncestors = Lists.reverse(ancestorPaths);
      for (Path ancestor : reversedAncestors) {
        config = findAndLoadGlobalConfigInDirectory(ancestor, config);
      }
    }
    return config;
  }

  /**
   * Find at most one *.properties file in the input {@link Path} and load it using fallback as fallback.
   * @return The {@link Config} in path with sysProps as fallback.
   * @throws IOException
   */
  private Config findAndLoadGlobalConfigInDirectory(Path path, Config fallback)
      throws IOException {
    FileStatus[] files = this.fs.listStatus(path, GLOBAL_PATH_FILTER);
    if (files == null) {
      log.warn("Could not list files at path " + path);
      return ConfigFactory.empty();
    }
    if (files.length > 1) {
      throw new IOException("Found more than one global properties file at path " + path);
    }
    if (files.length == 0) {
      return fallback;
    }
    if (GLOBAL_HOCON_PATH_FILTER.accept(files[0].getPath())) {
      return loadHoconConfigWithFallback(files[0].getPath(), fallback);
    } else if (GLOBAL_PROPS_PATH_FILTER.accept(files[0].getPath())) {
      return loadJavaPropsWithFallback(files[0].getPath(), fallback);
    } else {
      throw new IllegalStateException("Unsupported global configuration file: " + files[0].getPath());
    }
  }

  /**
   * Load a {@link Properties} compatible path using fallback as fallback.
   * @return The {@link Config} in path with fallback as fallback.
   * @throws IOException
   */
  private Config loadJavaPropsWithFallback(Path propertiesPath, Config fallback)
      throws IOException {

    PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    try (InputStreamReader inputStreamReader = new InputStreamReader(this.fs.open(propertiesPath), Charsets.UTF_8)) {
      propertiesConfiguration.setDelimiterParsingDisabled(ConfigUtils
          .getBoolean(fallback, PROPERTY_DELIMITER_PARSING_ENABLED_KEY, DEFAULT_PROPERTY_DELIMITER_PARSING_ENABLED_KEY));
      propertiesConfiguration.load(inputStreamReader);

      Config configFromProps =
          ConfigUtils.propertiesToConfig(ConfigurationConverter.getProperties(propertiesConfiguration));

      return ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY,
          PathUtils.getPathWithoutSchemeAndAuthority(propertiesPath).toString())).withFallback(configFromProps)
          .withFallback(fallback);
    } catch (ConfigurationException ce) {
      log.error("Failed to load Java properties from file at {} due to {}", propertiesPath, ce.getLocalizedMessage());
      throw new IOException(ce);
    }
  }

  private Config loadHoconConfigAtPath(Path path)
      throws IOException {
    try (InputStream is = fs.open(path); Reader reader = new InputStreamReader(is, Charsets.UTF_8)) {
      return ConfigFactory.parseMap(ImmutableMap
          .of(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, PathUtils.getPathWithoutSchemeAndAuthority(path).toString()))
          .withFallback(ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)));
    } catch (ConfigException configException) {
      throw wrapConfigException(path, configException);
    }
  }

  /**
   * Wrap a {@link ConfigException} (which extends {@link RuntimeException} with an IOException,
   * with a helpful message if possible
   * @param path
   * @param configException
   * @return an {@link IOException} wrapping the passed in ConfigException.
   */
  private IOException wrapConfigException(Path path, ConfigException configException) {
    if (configException.origin() != null) {
      return new IOException("Failed to parse config file " + path.toString()
          + " at lineNo:" + configException.origin().lineNumber(), configException);
    } else {
      return new IOException("Failed to parse config file " + path.toString(), configException);
    }
  }

  private Config loadHoconConfigWithFallback(Path path, Config fallback)
      throws IOException {
    try (InputStream is = fs.open(path); Reader reader = new InputStreamReader(is, Charsets.UTF_8)) {
      return ConfigFactory.parseMap(ImmutableMap
          .of(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, PathUtils.getPathWithoutSchemeAndAuthority(path).toString()))
          .withFallback(ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))).withFallback(fallback);
    } catch (ConfigException configException) {
      throw wrapConfigException(path, configException);
    }
  }

  private static class PathWithTimeStamp {
    long timeStamp;
    Path path;

    public PathWithTimeStamp(long timeStamp, Path path) {
      this.timeStamp = timeStamp;
      this.path = path;
    }
  }

  private static class ConfigWithTimeStamp {
    long timeStamp;
    Config config;

    public ConfigWithTimeStamp(long timeStamp, Config config) {
      this.timeStamp = timeStamp;
      this.config = config;
    }
  }
}
