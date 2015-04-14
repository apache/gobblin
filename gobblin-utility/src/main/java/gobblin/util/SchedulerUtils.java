/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;


/**
 * A utility class used by the scheduler.
 *
 * @author ynli
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
      rootProps.putAll(ConfigurationConverter
          .getProperties(new PropertiesConfiguration(new File(jobConfigDir, propertiesFiles[0]))));
    }

    String[] names = jobConfigDir.list();
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
    Iterable<String> jobConfigFileExtensionsIterable = Splitter.on(",").omitEmptyStrings().trimResults().split(
        properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS));
    return Sets.newHashSet(Iterables.transform(jobConfigFileExtensionsIterable, new Function<String, String>() {
          @Override
          public String apply(String input) {
            return input.toLowerCase();
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
