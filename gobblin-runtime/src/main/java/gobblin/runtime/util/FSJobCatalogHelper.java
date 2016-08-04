/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.util;

import gobblin.util.resourcesBasedTemplate;
import java.net.URI;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.io.OutputStream;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.typesafe.config.Config;
import com.google.common.collect.Lists;
import com.google.common.base.Splitter;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import gobblin.util.ConfigUtils;
import gobblin.runtime.api.JobSpec;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationDetector;
import gobblin.util.filesystem.PathAlterationObserver;
import gobblin.runtime.job_catalog.FSJobCatalog.Action;


/**
 * Pretty much the same as SchedulerUtils.java at current stage.
 * Todo: To completely migrate those parts of code into refactored job launcher.
 */

public class FSJobCatalogHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalogHelper.class);

  // Extension of properties files
  public static final String JOB_PROPS_FILE_EXTENSION = "properties";

  // Key used in the metadata of JobSpec.
  private static final String DESCRIPTION_KEY_IN_JOBSPEC = "description";
  private static final String VERSION_KEY_IN_JOBSPEC = "version";
  private static final String MERGED_INDICATOR_KEY_IN_JOBSPEC = "merged";
  private static final String MATERIALIZED_FILE_EXT = ".pull" ;

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
   *                   A good examplfied option of this is JOB_CONFIG_FILE_GENERAL_PATH_KEY
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<JobSpec> loadGenericJobConfigs(Properties properties)
      throws ConfigurationException, IOException {
    List<JobSpec> jobConfigs = Lists.newArrayList();
    loadGenericJobConfigsRecursive(jobConfigs, properties, properties, getJobConfigurationFileExtensions(properties),
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
  public static List<JobSpec> loadGenericJobConfigs(Properties properties, Path commonPropsPath, Path jobConfigPathDir)
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

    List<JobSpec> jobConfigs = Lists.newArrayList();
    // The common properties file will be loaded here
    loadGenericJobConfigsRecursive(jobConfigs, commonProps, properties, getJobConfigurationFileExtensions(properties),
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
  public static JobSpec loadGenericJobConfig(Properties properties, Path jobConfigPath, Path jobConfigPathDir)
      throws ConfigurationException, IOException {
    List<Properties> commonPropsList = Lists.newArrayList();

    // Attemptively load the jobConfig in target path, to see it contains MERGED_INDICATOR_KEY_IN_JOBSPEC option.
    Properties tmpJobPros = new Properties();
    tmpJobPros.putAll(ConfigurationConverter.getProperties(
        new PropertiesConfiguration(new Path("file://", jobConfigPath).toUri().toURL())));
    if (!tmpJobPros.containsKey(MERGED_INDICATOR_KEY_IN_JOBSPEC)) {
      getCommonProperties(commonPropsList, jobConfigPathDir, jobConfigPath.getParent());
    }
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

    if (jobProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
      jobProps = (new resourcesBasedTemplate(
          jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH))).getResolvedConfigAsProperties(jobProps);
    }

    jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, jobConfigPath.toString());

    // Create a JobSpec instance based on the info known.
    return jobSpecConverter(jobProps, jobConfigPath.toUri());
  }

  /**
   * Recursively load job configuration files under given URI of directory of config files folder
   * @param jobConfigs The target JobSpec List to be filled
   * @param rootProps The collected .properties file from ancestor directory.
   * @param frameworkProps The fundamental framwork properties.
   *                       even for those JobSpec submitted thru  {@link gobblin.runtime.api.JobSpecMonitor} Monitor,
   *                       this is still required and necessary.
   * @param jobConfigFileExtensions
   * @param configDirPath
   * @throws ConfigurationException
   * @throws IOException
   */
  private static void loadGenericJobConfigsRecursive(List<JobSpec> jobConfigs, Properties rootProps,
      Properties frameworkProps, Set<String> jobConfigFileExtensions, Path configDirPath)
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
          loadGenericJobConfigsRecursive(jobConfigs, rootPropsCopy, frameworkProps, jobConfigFileExtensions,
              configFilePath);
        } else {
          // Unsupported extension
          if (!jobConfigFileExtensions.contains(
              configFilePath.getName().substring(configFilePath.getName().lastIndexOf('.') + 1).toLowerCase())) {
            LOGGER.warn("Skipped file " + configFilePath + " that has an unsupported extension");
            continue;
          }

          // Deal with .done file, skip them.
          Path doneFilePath = configFilePath.suffix(".done");
          if (filesystem.exists(doneFilePath)) {
            LOGGER.info("Skipped job configuration file " + doneFilePath + " for which a .done file exists");
            continue;
          }

          // Only for those configuration files submitted by creating files in filesystem,
          // Put all parent/ancestor properties first.
          // For those submitted through external FSJobSpecMonitor, the loading of .properties file is unnecessary.
          // First attemptively load the target jobConfig, to see if there's MERGED_INDICATOR_KEY_IN_JOBSPEC contained.
          // This attemptive props is necessay as we need to make sure the common .properties is loaded firstly,
          // followed with user-specified option
          Properties attemptiveProps = new Properties();
          PropertiesConfiguration attemptivePropConfig = new PropertiesConfiguration();
          try (InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(configFilePath),
              Charsets.UTF_8)) {
            attemptivePropConfig.load(inputStreamReader);
            attemptiveProps.putAll(ConfigurationConverter.getProperties(attemptivePropConfig));
          }

          Properties jobProps = new Properties();
          if (attemptiveProps.containsKey(MERGED_INDICATOR_KEY_IN_JOBSPEC) && attemptiveProps.getProperty(
              MERGED_INDICATOR_KEY_IN_JOBSPEC).equals("true")) {
            jobProps.putAll(frameworkProps);
          } else {
            jobProps.putAll(rootProps);
          }

          // Then load the job configuration properties defined in the job configuration file
          PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
          try (InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(configFilePath),
              Charsets.UTF_8)) {

            propertiesConfiguration.load(inputStreamReader);
            jobProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));
            jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, configFilePath.toString());

            // Building and inserting the JobSpec accordingly.
            jobConfigs.add(jobSpecConverter(jobProps, configFilePath.toUri()));
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

  /**
   * For a specific job configuration file, from the folder it resides, collect all .properties file in commonPropsList.
   * @param commonPropsList The propList to be filled
   * @param jobConfigPathDir The job configuration path directory path.
   * @param configPathParent The target configuration file's parent directory path.
   * @throws ConfigurationException
   * @throws IOException
   */
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
      while (!configPathParent.equals(jobConfigPathDir.getParent())) {
        // Get the properties file that ends with .properties if any
        FileStatus[] propertiesFilesStatus = fileSystem.listStatus(configPathParent, PROPERTIES_PATH_FILTER);
        ArrayList<String> propertiesFilesList = new ArrayList<>();
        for (FileStatus propertiesFileStatus : propertiesFilesStatus) {
          propertiesFilesList.add(propertiesFileStatus.getPath().getName());
        }
        String[] propertiesFiles = propertiesFilesList.toArray(new String[propertiesFilesList.size()]);

        if (propertiesFiles.length > 0) {
          // There should be a single properties file in each directory (or sub directory)
          if (propertiesFiles.length != 1) {
            throw new RuntimeException("Found more than one .properties file in directory: " + configPathParent);
          }
          commonPropsList.add(ConfigurationConverter.getProperties(
              new PropertiesConfiguration((new Path(configPathParent, propertiesFiles[0])).toUri().toURL())));
        }

        configPathParent = configPathParent.getParent();
      }
    }
  }

  /**
   * To convert a JobSpec list into a HashMap with URI as the primary key.
   * Prepared to cache implementation.
   * @param jobSpecList
   * @return
   */
  public static HashMap<URI, JobSpec> jobSpecListToMap(List<JobSpec> jobSpecList) {
    HashMap<URI, JobSpec> persistedJob = new HashMap<>();
    if (jobSpecList == null || jobSpecList.size() == 0) {
      return persistedJob;
    } else {

      for (JobSpec aJobSpec : jobSpecList) {
        persistedJob.put(aJobSpec.getUri(), aJobSpec);
      }
    }
    return persistedJob;
  }

  /**
   * Combine two approaches of loading job configuration (both single conf. and conf. Folder) together.
   * @param jobConfigDirPath The path of root directory where configuration file reside.
   * @param action Enum type, indicating whether process is applied for a single configuration file,
   *               or a bunch of for a directory.
   * @param jobConfigPath The single job configuration file path if loading single configuration file,
   *                      only used for single configuration file loading therefore use {@link Optional}
   * @return A list of JobSpec for loading configuration files inside a directory
   *         One-element List of JobSpec for loading single configuration file, accessed by list.get(0).
   */
  public static List<JobSpec> loadJobConfigHelper(Path jobConfigDirPath, Action action, Optional<Path> jobConfigPath) {
    List<JobSpec> jobSpecList = new ArrayList<>();
    try (FileSystem fs = jobConfigDirPath.getFileSystem(new Configuration())) {
      // initialization of target folder.
      if (fs.exists(jobConfigDirPath)) {
        LOGGER.info("Loading job configurations from " + jobConfigDirPath);
        Properties properties = new Properties();
        // Temporarily adding this to make sure backward compatibility.
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, jobConfigDirPath.toString());
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, jobConfigDirPath.toString());
        switch (action) {
          case BATCHJOB:
            LOGGER.info("Loaded " + jobSpecList.size() + " job configuration(s)");
            jobSpecList = FSJobCatalogHelper.loadGenericJobConfigs(properties);
            break;
          case SINGLEJOB:
            if (jobConfigPath.isPresent()) {
              LOGGER.info("Loaded a job configuration : " + jobConfigPath.get());
              jobSpecList.add(
                  FSJobCatalogHelper.loadGenericJobConfig(properties, jobConfigPath.get(), jobConfigDirPath));
            } else {
              throw new RuntimeException(" Loading single job configuration but no Single-File path provided. ");
            }
            break;
          default:
            break;
        }
      } else {
        LOGGER.warn("Job configuration directory " + jobConfigDirPath + " not found");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to get the file system info based on configuration file" + jobConfigDirPath);
    } catch (ConfigurationException e) {
      throw new RuntimeException("Failed to load job configuration from" + jobConfigDirPath);
    }

    return jobSpecList;
  }

  /**
   * Add {@link PathAlterationDetector}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link PathAlterationDetector}.
   *
   * @param monitor a {@link PathAlterationDetector}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param rootDirPath root directory
   */
  public static void addPathAlterationObserver(PathAlterationDetector monitor, PathAlterationListener listener,
      Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer = new PathAlterationObserver(rootDirPath);
    observer.addListener(listener);
    monitor.addObserver(observer);
  }

  /**
   * On receiving a JobSpec object, materialized it into a real file into non-volatile storage layer.
   * The materialized file should be with the extension MATERIALIZED_FILE_EXT
   * @param jobSpec a {@link JobSpec}
   */
  public static void materializedJobSpec(JobSpec jobSpec)         
      throws IOException {
    Path path = new Path(jobSpec.getUri() + MATERIALIZED_FILE_EXT);
    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      Properties props = ConfigUtils.configToProperties(jobSpec.getConfig());

      // Persist the metadata info as well.
      props.setProperty(DESCRIPTION_KEY_IN_JOBSPEC, jobSpec.getDescription());
      props.setProperty(VERSION_KEY_IN_JOBSPEC, jobSpec.getVersion());

      try (OutputStream os = fs.create(path)) {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os, Charsets.UTF_8));
        props.store(bufferedWriter, "The jobSpec with URI[" + jobSpec.getUri() + "] has been materialized.");
      }
    }
  }

  /**
   * Tranforming a file into JobSpec object, identified by its URI.
   * This function is most used for testing usage, working with materialized method.
   * @param uri {@link URI} of the target to demateriaze.
   * @return a {@link JobSpec}
   */
  public static JobSpec dematerializeConfigFile(URI uri)
      throws IOException {
    Path path = new Path(uri);
    Properties props = new Properties();

    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      if (!fs.exists(path)) {
        throw new RuntimeException("The requested URI:" + uri + " doesn't exist");
      }
      try (InputStream is = fs.open(path)) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Charsets.UTF_8));
        props.load(bufferedReader);
      }
    }
    return jobSpecConverter(props, uri);
  }

  /**
   * Convert a raw jobProp that loaded from configuration file into JobSpec Object.
   * May remove some metadata inside, if the configuration file is originally
   * materialized from a JobSpec.
   *
   * @param rawProp The properties object that directly loaded from configuration file.
   * @param uri The uri of target job configuration file.
   * @return
   */
  public static JobSpec jobSpecConverter(Properties rawProp, URI uri) {
    Optional<String> description;
    Optional<String> version;
    Config config;
    if (rawProp != null) {
      // No exception thrown when the property not existed.
      description = Optional.fromNullable(rawProp.getProperty(DESCRIPTION_KEY_IN_JOBSPEC));
      if (description.isPresent()) {
        rawProp.remove(DESCRIPTION_KEY_IN_JOBSPEC);
      }
      version = Optional.fromNullable(rawProp.getProperty(VERSION_KEY_IN_JOBSPEC));
      if (version.isPresent()) {
        rawProp.remove(VERSION_KEY_IN_JOBSPEC);
      }
      config = ConfigUtils.propertiesToConfig(rawProp);
    } else {
      throw new RuntimeException("The null properties Object is being processed, error in loading");
    }

    return JobSpec.builder(uri)
        .withConfigAsProperties(rawProp)
        .withConfig(config)
        .withDescription(description.get())
        .withVersion(version.get())
        .build();
  }

  /**
   * Add addtional option indicating the jobSpec doesn't need implicitly loaded common properties.
   * @param jobSpec The origianl {@link JobSpec} created by external FS Monitor
   * @return Resolved JobSpec.
   */
  public static JobSpec markMergedOption(JobSpec jobSpec) {
    Optional<Properties> properties = Optional.fromNullable(jobSpec.getConfigAsProperties());
    Config config = jobSpec.getConfig();
    if (properties.isPresent()) {
      properties.get().setProperty(MERGED_INDICATOR_KEY_IN_JOBSPEC, "true");
    }
    config = ConfigUtils.propertiesToConfig(
        (Properties) ConfigUtils.configToProperties(config).setProperty(MERGED_INDICATOR_KEY_IN_JOBSPEC, "true"));

    return JobSpec.builder(jobSpec.getUri())
        .withDescription(jobSpec.getDescription())
        .withVersion(jobSpec.getVersion())
        .withConfig(config)
        .withConfigAsProperties(properties.get())
        .build();
  }
}
