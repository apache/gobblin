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

import com.google.common.base.Charsets;
import gobblin.configuration.ConfigurationKeys;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


/**
 * A utility class used by the scheduler.
 *
 * @author Yinan Li
 */
public class SchedulerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);

  // Extension of properties files
  public static final String JOB_PROPS_FILE_EXTENSION = "properties";

  // A filter for properties files
  private static final FilenameFilter PROPERTIES_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File file, String name) {
      return Files.getFileExtension(name).equalsIgnoreCase(JOB_PROPS_FILE_EXTENSION);
    }
  };

  // A filter for non-properties files
  private static final FilenameFilter NON_PROPERTIES_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return !Files.getFileExtension(name).equalsIgnoreCase(JOB_PROPS_FILE_EXTENSION);
    }
  };

  private static final PathFilter PROPERTIES_PATH_FILTER =        new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
      return fileExtension.equalsIgnoreCase(JOB_PROPS_FILE_EXTENSION);
    }
  };

  private static final PathFilter NON_PROPERTIES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
      return !fileExtension.equalsIgnoreCase(JOB_PROPS_FILE_EXTENSION);
    }
  };

  /**
   * Load job configuration from job configuration files stored in general file system,
   * located by Path
   */
  public static List<Properties> loadGenericJobConfigs(Properties properties)
      throws ConfigurationException, IOException {
    List<Properties> jobConfigs = Lists.newArrayList();
    loadGenericJobConfigsRecursive(jobConfigs, properties, getJobConfigurationFileExtensions(properties),
        (new Path(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY))).toUri() );
    return jobConfigs;
  }


  /**
   * Load job configurations from job configuration files stored under the
   * root job configuration file directory.
   *
   * @param properties Gobblin framework configuration properties
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadJobConfigs(Properties properties)
      throws ConfigurationException {
    Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY),
        "Missing configuration property: " + ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY);

    List<Properties> jobConfigs = Lists.newArrayList();
    loadJobConfigsRecursive(jobConfigs, properties, getJobConfigurationFileExtensions(properties),
        new File(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)));
    return jobConfigs;
  }

  /**
   * Load job configurations from job configuration files affected by changes to the given common properties file.
   *
   * @param properties Gobblin framework configuration properties
   * @param commonPropsFile the common properties file with changes
   * @param jobConfigFileDir root job configuration file directory
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadJobConfigs(Properties properties, File commonPropsFile, File jobConfigFileDir)
      throws ConfigurationException, IOException {
    List<Properties> commonPropsList = Lists.newArrayList();
    // Start from the parent of parent of the changed common properties file to avoid
    // loading the common properties file here since it will be loaded below anyway
    getCommonProperties(commonPropsList, jobConfigFileDir, commonPropsFile.getParentFile().getParentFile());
    // Add the framework configuration properties to the end
    commonPropsList.add(properties);

    Properties commonProps = new Properties();
    // Include common properties in reverse order
    for (Properties pros : Lists.reverse(commonPropsList)) {
      commonProps.putAll(pros);
    }

    List<Properties> jobConfigs = Lists.newArrayList();
    // The common properties file will be loaded here
    loadJobConfigsRecursive(jobConfigs, commonProps, getJobConfigurationFileExtensions(properties),
        commonPropsFile.getParentFile());
    return jobConfigs;
  }

  /**
   * Load a given job configuration file.
   *
   * @param properties Gobblin framework configuration properties
   * @param jobConfigFile job configuration file to be loaded
   * @param jobConfigFileDir root job configuration file directory
   * @return a job configuration in the form of {@link java.util.Properties}
   */
  public static Properties loadJobConfig(Properties properties, File jobConfigFile, File jobConfigFileDir)
      throws ConfigurationException, IOException {
    List<Properties> commonPropsList = Lists.newArrayList();
    getCommonProperties(commonPropsList, jobConfigFileDir, jobConfigFile.getParentFile());
    // Add the framework configuration properties to the end
    commonPropsList.add(properties);

    Properties jobProps = new Properties();
    // Include common properties in reverse order
    for (Properties commonProps : Lists.reverse(commonPropsList)) {
      jobProps.putAll(commonProps);
    }

    // Then load the job configuration properties defined in the job configuration file
    jobProps.putAll(ConfigurationConverter.getProperties(new PropertiesConfiguration(jobConfigFile)));
    jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, jobConfigFile.getAbsolutePath());
    return jobProps;
  }

  /**
   * Add {@link org.apache.commons.io.monitor.FileAlterationMonitor}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link org.apache.commons.io.monitor.FileAlterationMonitor}.
   *
   * @param monitor a {@link org.apache.commons.io.monitor.FileAlterationMonitor}
   * @param listener a {@link org.apache.commons.io.monitor.FileAlterationListener}
   * @param rootDir root directory
   */
  public static void addFileAlterationObserver(FileAlterationMonitor monitor, FileAlterationListener listener,
      File rootDir) {
    // Add a observer for the current root directory
    FileAlterationObserver observer = new FileAlterationObserver(rootDir);
    observer.addListener(listener);
    monitor.addObserver(observer);

    // List subdirectories under the current root directory
    File[] subDirs = rootDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isDirectory();
      }
    });

    if (subDirs == null || subDirs.length == 0) {
      return;
    }

    // Recursively add a observer for each subdirectory
    for (File subDir : subDirs) {
      addFileAlterationObserver(monitor, listener, subDir);
    }
  }

  /**
   * Recursively load job configuration files under given URI of **directory of config files folder**
   * todo
   */
  private static void loadGenericJobConfigsRecursive(List<Properties> jobConfigs, Properties rootProps,
      Set<String> jobConfigFileExtensions, URI uri)
      throws ConfigurationException, IOException {

    /* create the generic file system */
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    FileSystem filesystem = FileSystem.get(uri, conf);

    Path configDirPath = new Path(uri);
    if (!filesystem.exists(configDirPath)) {
      throw new RuntimeException("No specified .properties directory found :" + configDirPath.toString());
    }

    FileStatus[] propertiesFilesStatus = filesystem.listStatus(configDirPath, PROPERTIES_PATH_FILTER);
    ArrayList<String> propertiesFiles = new ArrayList<>();
    for (FileStatus file : propertiesFilesStatus) {
      propertiesFiles.add(file.getPath().getName());
    }
    if (propertiesFiles.size() > 0) {
      // There should be a single properties file in each directory (or sub directory)
      if (propertiesFiles.size() != 1) {
        throw new RuntimeException("Found more than one .properties file in directory: " + configDirPath);
      }

      // Load the properties, which may overwrite the same properties defined in the parent or ancestor directories.
      // Open the inputStream, construct a reader and send to the loader for constructing propertiesConfiguration.
      PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
      Path uniqueConfigFilePath = configDirPath.suffix(propertiesFiles.get(0));
      InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(uniqueConfigFilePath), Charsets.UTF_8);
      propertiesConfiguration.load(inputStreamReader);

      rootProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));
      inputStreamReader.close();
    }

    // Get all non-properties files
    FileStatus[] nonPropFiles = filesystem.listStatus(configDirPath, NON_PROPERTIES_PATH_FILTER);
    ArrayList<String> names = new ArrayList<>();
    for (FileStatus nonPropFile : nonPropFiles) {
      names.add(nonPropFile.getPath().getName());
    }
    if (names.size() == 0 ) {
      return;
    }

    for (String name : names) {
      Path configFilePath = new Path(configDirPath + "/" + name );
      FileStatus configFileStatus = filesystem.getFileStatus(configFilePath);
      if (configFileStatus.isDirectory()) {
        Properties rootPropsCopy = new Properties();
        rootPropsCopy.putAll(rootProps);
        loadGenericJobConfigsRecursive(jobConfigs, rootPropsCopy, jobConfigFileExtensions, uri);
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
        InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(configFilePath), Charsets.UTF_8);
        propertiesConfiguration.load(inputStreamReader);

        jobProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));
        jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, uri.toString() );
        jobConfigs.add(jobProps);

        inputStreamReader.close();
      }
    }

    filesystem.close();
  }

  /**
   * Recursively load job configuration files under the given directory.
   */
  private static void loadJobConfigsRecursive(List<Properties> jobConfigs, Properties rootProps,
      Set<String> jobConfigFileExtensions, File jobConfigDir)
      throws ConfigurationException {

    // Get the properties file that ends with .properties if any
    String[] propertiesFiles = jobConfigDir.list(PROPERTIES_FILE_FILTER);
    if (propertiesFiles != null && propertiesFiles.length > 0) {
      // There should be a single properties file in each directory (or sub directory)
      if (propertiesFiles.length != 1) {
        throw new RuntimeException("Found more than one .properties file in directory: " + jobConfigDir);
      }

      // Load the properties, which may overwrite the same properties defined in the parent or ancestor directories.
      rootProps.putAll(ConfigurationConverter.getProperties(
          new PropertiesConfiguration(new File(jobConfigDir, propertiesFiles[0]))));
    }

    // Get all non-properties files
    String[] names = jobConfigDir.list(NON_PROPERTIES_FILE_FILTER);
    if (names == null || names.length == 0) {
      return;
    }

    for (String name : names) {
      File file = new File(jobConfigDir, name);
      if (file.isDirectory()) {
        Properties rootPropsCopy = new Properties();
        rootPropsCopy.putAll(rootProps);
        loadJobConfigsRecursive(jobConfigs, rootPropsCopy, jobConfigFileExtensions, file);
      } else {
        if (!jobConfigFileExtensions.contains(Files.getFileExtension(file.getName()).toLowerCase())) {
          LOGGER.warn("Skipped file " + file + " that has an unsupported extension");
          continue;
        }

        File doneFile = new File(file + ".done");
        if (doneFile.exists()) {
          // Skip the job configuration file when a .done file with the same name exists,
          // which means the job configuration file is for a one-time job and the job has
          // already run and finished.
          LOGGER.info("Skipped job configuration file " + file + " for which a .done file exists");
          continue;
        }

        Properties jobProps = new Properties();
        // Put all parent/ancestor properties first
        jobProps.putAll(rootProps);
        // Then load the job configuration properties defined in the job configuration file
        jobProps.putAll(ConfigurationConverter.getProperties(new PropertiesConfiguration(file)));
        jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, file.getAbsolutePath());
        jobConfigs.add(jobProps);
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

  private static void getCommonProperties(List<Properties> commonPropsList, File jobConfigFileDir, File dir)
      throws ConfigurationException, IOException {
    // Make sure the given starting directory is under the job configuration file directory
    Preconditions.checkArgument(dir.getCanonicalPath().startsWith(jobConfigFileDir.getCanonicalPath()),
        String.format("%s is not an ancestor directory of %s", jobConfigFileDir, dir));

    // Traversal backward until the parent of the root job configuration file directory is reached
    while (!dir.equals(jobConfigFileDir.getParentFile())) {
      // Get the properties file that ends with .properties if any
      String[] propertiesFiles = dir.list(PROPERTIES_FILE_FILTER);
      if (propertiesFiles != null && propertiesFiles.length > 0) {
        // There should be a single properties file in each directory (or sub directory)
        if (propertiesFiles.length != 1) {
          throw new RuntimeException("Found more than one .properties file in directory: " + dir);
        }
        commonPropsList.add(
            ConfigurationConverter.getProperties(new PropertiesConfiguration(new File(dir, propertiesFiles[0]))));
      }

      dir = dir.getParentFile();
    }
  }
}
