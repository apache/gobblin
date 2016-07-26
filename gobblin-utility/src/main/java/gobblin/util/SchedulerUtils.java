/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationMonitor;
import gobblin.util.filesystem.PathAlterationObserver;
import gobblin.runtime.util.SimpleGeneralJobTemplate;


/**
 * A utility class used by the scheduler.
 *
 * @author Yinan Li
 */
public class SchedulerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);

  // Extension of properties files
  public static final String JOB_PROPS_FILE_EXTENSION = "properties";

  private static final PathFilter PROPERTIES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
      return fileExtension.equalsIgnoreCase(JOB_PROPS_FILE_EXTENSION);
    }
  };

  private static final PathFilter NON_PROPERTIES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !PROPERTIES_PATH_FILTER.accept(path);
    }
  };

  /**
   * Load job configuration from job configuration files stored in general file system,
   * located by Path
   * @param properties Gobblin framework configuration properties
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadGenericJobConfigs(Properties properties)
      throws ConfigurationException, IOException {
    List<Properties> jobConfigs = Lists.newArrayList();
    loadGenericJobConfigsRecursive(jobConfigs, properties, getJobConfigurationFileExtensions(properties),
        new Path(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)));
    return jobConfigs;
  }

  /**
   * Load job configurations from job configuration files affected by changes to the given common properties file.
   * From a general file system.
   * @param properties Gobblin framework configuration properties
   * @param commonPropsPath the path of common properties file with changes
   * @param jobConfigPathDir the path for root job configuration file directory
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadGenericJobConfigs(Properties properties, Path commonPropsPath,
      Path jobConfigPathDir)
      throws ConfigurationException, IOException {
    List<Properties> commonPropsList = Lists.newArrayList();
    // Start from the parent of parent of the changed common properties file to avoid
    // loading the common properties file here since it will be loaded below anyway
    getCommonProperties(commonPropsList, jobConfigPathDir, commonPropsPath.getParent().getParent());
    // Add the framework configuration properties to the end
    commonPropsList.add(properties);

    Properties commonProps = new Properties();
    // Include common properties in reverse order
    for (Properties pros : Lists.reverse(commonPropsList)) {
      commonProps.putAll(pros);
    }

    List<Properties> jobConfigs = Lists.newArrayList();
    // The common properties file will be loaded here
    loadGenericJobConfigsRecursive(jobConfigs, commonProps, getJobConfigurationFileExtensions(properties),
        commonPropsPath.getParent());
    return jobConfigs;
  }

  /**
   * Load a given job configuration file from a general file system.
   *
   * @param properties Gobblin framework configuration properties
   * @param jobConfigPath job configuration file to be loaded
   * @param jobConfigPathDir root job configuration file directory
   * @return a job configuration in the form of {@link java.util.Properties}
   */
  public static Properties loadGenericJobConfig(Properties properties, Path jobConfigPath, Path jobConfigPathDir)
      throws ConfigurationException, IOException {
    List<Properties> commonPropsList = Lists.newArrayList();
    getCommonProperties(commonPropsList, jobConfigPathDir, jobConfigPath.getParent());
    // Add the framework configuration properties to the end
    commonPropsList.add(properties);

    Properties jobProps = new Properties();
    // Include common properties in reverse order
    for (Properties commonProps : Lists.reverse(commonPropsList)) {
      jobProps.putAll(commonProps);
    }

    // Then load the job configuration properties defined in the job configuration file
    jobProps.putAll(ConfigurationConverter.getProperties(
        new PropertiesConfiguration(new Path("file://", jobConfigPath).toUri().toURL())));

    jobProps = (new SimpleGeneralJobTemplate(ConfigurationKeys.JOB_TEMPLATE_PATH)).getResolvedConfig(jobProps);

    jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, jobConfigPath.toString());
    return jobProps;
  }

  /**
   * Add {@link gobblin.util.filesystem.PathAlterationMonitor}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link gobblin.util.filesystem.PathAlterationMonitor}.
   *
   * @param monitor a {@link gobblin.util.filesystem.PathAlterationMonitor}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param rootDirPath root directory
   */
  public static void addPathAlterationObserver(PathAlterationMonitor monitor, PathAlterationListener listener,
      Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer = new PathAlterationObserver(rootDirPath);
    observer.addListener(listener);
    monitor.addObserver(observer);
  }

  /**
   * Recursively load job configuration files under given URI of directory of config files folder
   */
  private static void loadGenericJobConfigsRecursive(List<Properties> jobConfigs, Properties rootProps,
      Set<String> jobConfigFileExtensions, Path configDirPath)
      throws ConfigurationException, IOException {

    Configuration conf = new Configuration();
    try (FileSystem filesystem = configDirPath.getFileSystem(conf)) {
      if (!filesystem.exists(configDirPath)) {
        throw new RuntimeException(
            "The specified job configurations directory was not found: " + configDirPath.toString());
      }

      FileStatus[] propertiesFilesStatus = filesystem.listStatus(configDirPath, PROPERTIES_PATH_FILTER);
      if (propertiesFilesStatus != null && propertiesFilesStatus.length > 0) {
        // There should be a single properties file in each directory (or sub directory)
        if (propertiesFilesStatus.length != 1) {
          throw new RuntimeException("Found more than one .properties file in directory: " + configDirPath);
        }

        // Load the properties, which may overwrite the same properties defined in the parent or ancestor directories.
        // Open the inputStream, construct a reader and send to the loader for constructing propertiesConfiguration.
        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
        Path uniqueConfigFilePath = propertiesFilesStatus[0].getPath();
        try (InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(uniqueConfigFilePath),
            Charsets.UTF_8)) {
          propertiesConfiguration.load(inputStreamReader);
          rootProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));
        }
      }

      // Get all non-properties files
      FileStatus[] nonPropFiles = filesystem.listStatus(configDirPath, NON_PROPERTIES_PATH_FILTER);
      if (nonPropFiles == null || nonPropFiles.length == 0) {
        return;
      }

      for (FileStatus nonPropFile : nonPropFiles) {
        Path configFilePath = nonPropFile.getPath();
        if (nonPropFile.isDirectory()) {
          Properties rootPropsCopy = new Properties();
          rootPropsCopy.putAll(rootProps);
          loadGenericJobConfigsRecursive(jobConfigs, rootPropsCopy, jobConfigFileExtensions, configFilePath);
        } else {
          if (!jobConfigFileExtensions.contains(
              configFilePath.getName().substring(configFilePath.getName().lastIndexOf('.') + 1).toLowerCase())) {
            LOGGER.warn("Skipped file " + configFilePath + " that has an unsupported extension");
            continue;
          }

          Path doneFilePath = configFilePath.suffix(".done");
          if (filesystem.exists(doneFilePath)) {
            LOGGER.info("Skipped job configuration file " + doneFilePath + " for which a .done file exists");
            continue;
          }

          Properties jobProps = new Properties();
          // Put all parent/ancestor properties first
          jobProps.putAll(rootProps);
          // Then load the job configuration properties defined in the job configuration file
          PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
          try (InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(configFilePath),
              Charsets.UTF_8)) {
            propertiesConfiguration.load(inputStreamReader);
            jobProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));

            jobProps = (new SimpleGeneralJobTemplate(ConfigurationKeys.JOB_TEMPLATE_PATH)).getResolvedConfig(jobProps);

            jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, configFilePath.toString());
            jobConfigs.add(jobProps);
          }
        }
      }
    }
  }

  private static Set<String> getJobConfigurationFileExtensions(Properties properties) {
    Iterable<String> jobConfigFileExtensionsIterable = Splitter.on(",")
        .omitEmptyStrings()
        .trimResults()
        .split(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS));
    return ImmutableSet.copyOf(Iterables.transform(jobConfigFileExtensionsIterable, new Function<String, String>() {
      @Override
      public String apply(String input) {
        return null != input ? input.toLowerCase() : "";
      }
    }));
  }

  private static void getCommonProperties(List<Properties> commonPropsList, Path jobConfigPathDir,
      Path configPathParent)
      throws ConfigurationException, IOException {
    Configuration conf = new Configuration();
    try (FileSystem fileSystem = jobConfigPathDir.getFileSystem(conf)) {
      // Make sure the given starting directory is under the job configuration file directory
      Preconditions.checkArgument(
          configPathParent.toUri().normalize().getPath().startsWith(jobConfigPathDir.toUri().normalize().getPath()),
          String.format("%s is not an ancestor directory of %s", jobConfigPathDir, configPathParent));

      // Traversal backward until the parent of the root job configuration file directory is reached
      // Pay attention that the path Object will have a "file:/" as the prefix, so for comparison requirement
      // that prefix need to be addressed.
      while (!PathUtils.compareWithoutSchemeAndAuthority(configPathParent, jobConfigPathDir.getParent())) {

        // Get the properties file that ends with .properties if any
        FileStatus[] propertiesFilesStatus = fileSystem.listStatus(configPathParent, PROPERTIES_PATH_FILTER);
        ArrayList<String> propertiesFilesList = new ArrayList<>();

        if (propertiesFilesStatus != null && propertiesFilesStatus.length > 0) {
          for (FileStatus propertiesFileStatus : propertiesFilesStatus) {
            propertiesFilesList.add(propertiesFileStatus.getPath().getName());
          }
        }

        String[] propertiesFiles = propertiesFilesList.toArray(new String[propertiesFilesList.size()]);

        if (propertiesFiles != null && propertiesFiles.length > 0) {
          // There should be a single properties file in each directory (or sub directory)
          if (propertiesFiles.length != 1) {
            throw new RuntimeException("Found more than one .properties file in directory: " + configPathParent);
          }

          commonPropsList.add(ConfigurationConverter.getProperties(new PropertiesConfiguration(
              (new Path((new Path("file://", configPathParent)), propertiesFiles[0])).toUri().toURL())));
        }
        configPathParent = configPathParent.getParent();
      }
    }
  }
}
