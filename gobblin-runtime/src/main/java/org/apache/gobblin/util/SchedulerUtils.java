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

package gobblin.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import gobblin.runtime.template.ResourceBasedJobTemplate;
import gobblin.util.filesystem.PathAlterationObserverScheduler;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationObserver;


/**
 * A utility class used by the scheduler.
 *
 * @author Yinan Li
 */
public class SchedulerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);

  // Extension of properties files
  public static final String JOB_PROPS_FILE_EXTENSION = "properties";

  /**
   * Load job configuration from job configuration files stored in general file system,
   * located by Path
   * @param sysProps Gobblin framework configuration properties
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadGenericJobConfigs(Properties sysProps)
      throws ConfigurationException, IOException {
    Path rootPath = new Path(sysProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    PullFileLoader loader = new PullFileLoader(rootPath, rootPath.getFileSystem(new Configuration()),
        getJobConfigurationFileExtensions(sysProps), PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
    Config sysConfig = ConfigUtils.propertiesToConfig(sysProps);
    Collection<Config> configs =
        loader.loadPullFilesRecursively(rootPath, sysConfig, true);

    List<Properties> jobConfigs = Lists.newArrayList();
    for (Config config : configs) {
      try {
        jobConfigs.add(resolveTemplate(ConfigUtils.configToProperties(config)));
      } catch (IOException ioe) {
        LOGGER.error("Could not parse job config at " + ConfigUtils.getString(config,
            ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, "Unknown path"), ioe);
      }
    }

    return jobConfigs;
  }

  /**
   * Load job configurations from job configuration files affected by changes to the given common properties file.
   * From a general file system.
   * @param sysProps Gobblin framework configuration properties
   * @param commonPropsPath the path of common properties file with changes
   * @param jobConfigPathDir the path for root job configuration file directory
   * @return a list of job configurations in the form of {@link java.util.Properties}
   */
  public static List<Properties> loadGenericJobConfigs(Properties sysProps, Path commonPropsPath,
      Path jobConfigPathDir)
      throws ConfigurationException, IOException {

    PullFileLoader loader = new PullFileLoader(jobConfigPathDir, jobConfigPathDir.getFileSystem(new Configuration()),
        getJobConfigurationFileExtensions(sysProps), PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
    Config sysConfig = ConfigUtils.propertiesToConfig(sysProps);
    Collection<Config> configs =
        loader.loadPullFilesRecursively(commonPropsPath.getParent(), sysConfig, true);

    List<Properties> jobConfigs = Lists.newArrayList();
    for (Config config : configs) {
      try {
        jobConfigs.add(resolveTemplate(ConfigUtils.configToProperties(config)));
      } catch (IOException ioe) {
        LOGGER.error("Could not parse job config at " + ConfigUtils.getString(config,
            ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, "Unknown path"), ioe);
      }
    }

    return jobConfigs;
  }

  /**
   * Load a given job configuration file from a general file system.
   *
   * @param sysProps Gobblin framework configuration properties
   * @param jobConfigPath job configuration file to be loaded
   * @param jobConfigPathDir root job configuration file directory
   * @return a job configuration in the form of {@link java.util.Properties}
   */
  public static Properties loadGenericJobConfig(Properties sysProps, Path jobConfigPath, Path jobConfigPathDir)
      throws ConfigurationException, IOException {

    PullFileLoader loader = new PullFileLoader(jobConfigPathDir, jobConfigPathDir.getFileSystem(new Configuration()),
        getJobConfigurationFileExtensions(sysProps), PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);

    Config sysConfig = ConfigUtils.propertiesToConfig(sysProps);
    Config config = loader.loadPullFile(jobConfigPath, sysConfig, true);
    return resolveTemplate(ConfigUtils.configToProperties(config));
  }

  /**
   * Add {@link PathAlterationObserverScheduler}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link PathAlterationObserverScheduler}.
   *
   * @param monitor a {@link PathAlterationObserverScheduler}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param rootDirPath root directory
   */
  public static void addPathAlterationObserver(PathAlterationObserverScheduler monitor, PathAlterationListener listener,
      Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer = new PathAlterationObserver(rootDirPath);
    observer.addListener(listener);
    monitor.addObserver(observer);
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

  private static Properties resolveTemplate(Properties jobProps) throws IOException {
    try {
      if (jobProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
        Config jobConfig = ConfigUtils.propertiesToConfig(jobProps);
        Properties resolvedProps = ConfigUtils.configToProperties((ResourceBasedJobTemplate
            .forResourcePath(jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH),
                new PackagedTemplatesJobCatalogDecorator()))
            .getResolvedConfig(jobConfig));
        return resolvedProps;
      } else {
        return jobProps;
      }
    } catch (JobTemplate.TemplateException | SpecNotFoundException | URISyntaxException exc) {
      throw new IOException(exc);
    }
  }
}
