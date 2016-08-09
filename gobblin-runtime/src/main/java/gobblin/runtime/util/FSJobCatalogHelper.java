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

import java.net.URI;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.io.OutputStream;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import com.google.common.collect.ImmutableSet;

import gobblin.util.ConfigUtils;
import gobblin.runtime.api.JobSpec;
import gobblin.util.ResourceBasedTemplate;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationDetector;
import gobblin.util.filesystem.PathAlterationObserver;
import gobblin.runtime.job_catalog.FSJobCatalog.Action;


/**
 * Pretty much the same as SchedulerUtils.java at current stage.
 * Todo: To completely migrate those parts of code into refactored job launcher & scheduler .
 */

public class FSJobCatalogHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalogHelper.class);

  // Key used in the metadata of JobSpec.
  private static final String DESCRIPTION_KEY_IN_JOBSPEC = "description";
  private static final String VERSION_KEY_IN_JOBSPEC = "version";
  // Extension of properties files
  public static final String JOB_PROPS_FILE_EXTENSION = "properties";

  /**
   * For backward compatibility, to ignore *.properties files.
   */
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
   * @return a list of job configurations in the form of {@link JobSpec}
   */
  public static List<JobSpec> loadGenericJobConfigs(Properties properties)
      throws ConfigurationException, IOException {
    List<JobSpec> jobConfigs = Lists.newArrayList();
    loadGenericJobConfigsRecursive(jobConfigs, properties, getJobConfigurationFileExtensions(properties),
        new Path(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)));
    return jobConfigs;
  }

  /**
   * Load a given job configuration file from a general file system.
   *
   * @param frameworkProps
   * @param jobConfigRelativePath
   * @param jobConfigDirPath
   * @return a single job configuration in the form of {@link JobSpec}
   */

  /**
   *
   * @param frameworkProps Gobblin framework configuration properties
   * @param jobConfigRelativePath job configuration file to be loaded
   * @param jobConfigDirPath The root job configuration directory path
   * @param augmentEnabled For general usage we need to add framworkProps as well as provide resourcesBased template
   *                       For testing of de-materialization, these are not necessary.
   * @return
   * @throws ConfigurationException
   * @throws IOException
   */
  public static JobSpec loadGenericJobConfig(Properties frameworkProps, Path jobConfigRelativePath,
      Path jobConfigDirPath, boolean augmentEnabled)
      throws ConfigurationException, IOException {
    // Add the framework configuration properties to the end
    Properties jobProps = new Properties();
    if (augmentEnabled) {
      jobProps.putAll(frameworkProps);
    }

    // Assemble the complete path in target file system first.
    // Then load the job configuration properties defined in the job configuration file
    Path completePath = new Path(jobConfigDirPath, jobConfigRelativePath);

    try (FileSystem fs = completePath.getFileSystem(new Configuration())) {
      if (!fs.exists(completePath)) {
        throw new RuntimeException("The requested URI:" + jobConfigRelativePath + " doesn't exist");
      }
      try (InputStream is = fs.open(completePath)) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Charsets.UTF_8));
        jobProps.load(bufferedReader);
        bufferedReader.close();
      }
    }

    // For all the non-test usages, this augmentEnabled is True.
    if (augmentEnabled) {
      if (jobProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
        jobProps = (new ResourceBasedTemplate(
            jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH))).getResolvedConfigAsProperties(jobProps);
      }

      jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, jobConfigRelativePath.toString());
    }

    // Create a JobSpec instance based on the info known.
    // Noted that jobConfigPath is the

    return jobSpecConverter(jobProps, jobConfigRelativePath.toUri());
  }

  /**
   * Recursively load job configuration files under given URI of directory of config files folder
   * @param jobConfigs The target JobSpec List to be filled
   * @param frameworkProps The fundamental framework properties.
   *                       Typical exmaple includes 'JOB_CONFIG_FILE_GENERAL_PATH_KEY'
   *                       even for those JobSpec submitted thru  {@link gobblin.runtime.api.JobSpecMonitor} Monitor,
   *                       this is still required and necessary.
   * @param jobConfigFileExtensions The set of extension that is accepted.
   * @param configDirPath The root directory for job configuration files.
   * @throws ConfigurationException
   * @throws IOException
   */
  private static void loadGenericJobConfigsRecursive(List<JobSpec> jobConfigs, Properties frameworkProps,
      Set<String> jobConfigFileExtensions, Path configDirPath)
      throws ConfigurationException, IOException {

    Configuration conf = new Configuration();
    try (FileSystem filesystem = configDirPath.getFileSystem(conf)) {
      if (!filesystem.exists(configDirPath)) {
        throw new RuntimeException(
            "The specified job configurations directory was not found: " + configDirPath.toString());
      }

      // Get all files's status
      // Ignore all the .properties files.
      FileStatus[] nonPropFiles = filesystem.listStatus(configDirPath, NON_PROPERTIES_PATH_FILTER);
      if (nonPropFiles == null || nonPropFiles.length == 0) {
        return;
      }

      for (FileStatus nonPropFile : nonPropFiles) {
        Path configFilePath = nonPropFile.getPath();
        // For directory recursively load the next level.
        // Make sure that the frameworkProps is applied when run into a file.
        if (nonPropFile.isDirectory()) {
          loadGenericJobConfigsRecursive(jobConfigs, frameworkProps, jobConfigFileExtensions, configFilePath);
        } else {
          // Unsupported extension
          if (!jobConfigFileExtensions.contains(
              configFilePath.getName().substring(configFilePath.getName().lastIndexOf('.') + 1).toLowerCase())) {
            LOGGER.warn("Skipped file " + configFilePath + " that has an unsupported extension");
            continue;
          }

          // Deal with .done file, skip them
          // (Legacy, supposed to be no .done file in this folder for this version).
          Path doneFilePath = configFilePath.suffix(".done");
          if (filesystem.exists(doneFilePath)) {
            LOGGER.info("Skipped job configuration file " + doneFilePath + " for which a .done file exists");
            continue;
          }

          Properties jobProps = new Properties();
          jobProps.putAll(frameworkProps);

          // Then load the job configuration properties defined in the job configuration file
          PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
          try (InputStreamReader inputStreamReader = new InputStreamReader(filesystem.open(configFilePath),
              Charsets.UTF_8)) {

            propertiesConfiguration.load(inputStreamReader);
            jobProps.putAll(ConfigurationConverter.getProperties(propertiesConfiguration));
            jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, configFilePath.toString());

            // Get template there's any corresponded.
            if (jobProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
              jobProps = (new ResourceBasedTemplate(
                  jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH))).getResolvedConfigAsProperties(jobProps);
            }

            // Building and inserting the JobSpec accordingly.
            // Keep the URI(identifier as the relative path. )
            try {
              jobConfigs.add(jobSpecConverter(jobProps, new URI(configFilePath.getName())));
            } catch (URISyntaxException e) {
              throw new RuntimeException(e.getMessage());
            }
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
   * Combine two approaches of loading job configuration (both single conf. and conf. Folder) together.
   * @param jobConfigDirPath The path of root directory where configuration file reside.
   * @param action Enum type, indicating whether process is applied for a single configuration file,
   *               or a bunch of for a directory.
   * @param jobConfigPath The single job configuration file path(Relative Path)
   *                      if loading single configuration file,
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
                  FSJobCatalogHelper.loadGenericJobConfig(properties, jobConfigPath.get(), jobConfigDirPath, true));
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
   * Create and attach {@link PathAlterationDetector}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link PathAlterationDetector}.
   * @param detector  a {@link PathAlterationDetector}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param observerOptional Optional observer object. For testing routine, this has been initialized by user.
   *                         But for general usage, the observer object is created inside this method.
   * @param rootDirPath root directory
   * @throws IOException
   */
  public static void addPathAlterationObserver(PathAlterationDetector detector, PathAlterationListener listener,
      Optional<PathAlterationObserver> observerOptional, Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer;
    if (observerOptional.isPresent()) {
      observer = observerOptional.get();
    } else {
      observer = new PathAlterationObserver(rootDirPath);
    }
    observer.addListener(listener);
    detector.addObserver(observer);
  }

  /**
   * On receiving a JobSpec object, materialized it into a real file into non-volatile storage layer.
   * @param jobConfDirPath The configuration root directory path.
   * @param jobSpec a {@link JobSpec}
   */
  public static void materializeJobSpec(Path jobConfDirPath, JobSpec jobSpec)
      throws IOException {
    Path path = new Path(jobConfDirPath, jobSpec.getUri().toString());
    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      Properties props = ConfigUtils.configToProperties(jobSpec.getConfig());

      // Persist the metadata info as well.
      props.setProperty(DESCRIPTION_KEY_IN_JOBSPEC, jobSpec.getDescription());
      props.setProperty(VERSION_KEY_IN_JOBSPEC, jobSpec.getVersion());

      try (OutputStream os = fs.create(path)) {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os, Charsets.UTF_8));
        props.store(bufferedWriter, "");
        bufferedWriter.close();
      }
    }
  }

  /**
   * Convert a raw jobProp that loaded from configuration file into JobSpec Object.
   * May remove some metadata inside, if the configuration file is originally
   * materialized from a JobSpec.
   *
   * @param rawProp The properties object that directly loaded from configuration file.
   * @param jobConfigURI The uri of target job configuration file.
   *                     Relative path to the root Configuration folder.
   * @return
   */
  public static JobSpec jobSpecConverter(Properties rawProp, URI jobConfigURI) {
    Optional<String> description = Optional.absent();
    Optional<Config> config;
    Optional<Properties> optionalProp;
    String version = "";

    if (rawProp != null) {
      version = rawProp.getProperty(VERSION_KEY_IN_JOBSPEC);

      // To ensure the transparency of JobCatalog, need to remove the addtional
      // options that added through the materialization process.
      rawProp.remove(VERSION_KEY_IN_JOBSPEC);

      // No exception thrown when the property not existed.
      description = Optional.fromNullable(rawProp.getProperty(DESCRIPTION_KEY_IN_JOBSPEC));
      if (description.isPresent()) {
        rawProp.remove(DESCRIPTION_KEY_IN_JOBSPEC);
      }
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
      }
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY);
      }
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY);
      }
    }
    optionalProp = Optional.fromNullable(rawProp);
    config = Optional.fromNullable(ConfigUtils.propertiesToConfig(rawProp));

    // The builder has null-checker. Leave the checking there.
    return JobSpec.builder(jobConfigURI)
        .withConfigAsProperties(optionalProp.get())
        .withConfig(config.get())
        .withDescription(description.get())
        .withVersion(version)
        .build();
  }
}
