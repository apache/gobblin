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

package gobblin.runtime.job_catalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.util.PathUtils;
import gobblin.util.PullFileLoader;
import gobblin.util.filesystem.PathAlterationObserverScheduler;
import gobblin.util.filesystem.PathAlterationObserver;

import lombok.AllArgsConstructor;
import lombok.Getter;


public class ImmutableFSJobCatalog extends JobCatalogBase implements JobCatalog {

  protected final FileSystem fs;
  protected final Config sysConfig;

  protected static final Logger LOGGER = LoggerFactory.getLogger(ImmutableFSJobCatalog.class);

  protected final PullFileLoader loader;

  /* The root configuration directory path.*/
  protected final Path jobConfDirPath;
  private final ImmutableFSJobCatalog.JobSpecConverter converter;

  // A monitor for changes to job conf files for general FS
  // This embedded monitor is monitoring job configuration files instead of JobSpec Object.
  protected final PathAlterationObserverScheduler pathAlterationDetector;
  public static final String FS_CATALOG_KEY_PREFIX = "gobblin.fsJobCatalog";
  public static final String VERSION_KEY_IN_JOBSPEC = "gobblin.fsJobCatalog.version";
  // Key used in the metadata of JobSpec.
  public static final String DESCRIPTION_KEY_IN_JOBSPEC = "gobblin.fsJobCatalog.description";

  public ImmutableFSJobCatalog(Config sysConfig)
      throws IOException {
    this(sysConfig, null);
  }

  public ImmutableFSJobCatalog(GobblinInstanceEnvironment env)
      throws IOException {
    this(env.getSysConfig().getConfig(), null, Optional.of(env.getMetricContext()),
         env.isInstrumentationEnabled());
  }

  public ImmutableFSJobCatalog(GobblinInstanceEnvironment env, PathAlterationObserver observer)
      throws IOException {
    this(env.getSysConfig().getConfig(), observer, Optional.of(env.getMetricContext()),
         env.isInstrumentationEnabled());
  }

  public ImmutableFSJobCatalog(Config sysConfig, PathAlterationObserver observer)
      throws IOException {
    this(sysConfig, observer, Optional.<MetricContext>absent(), GobblinMetrics.isEnabled(sysConfig));
  }

  public ImmutableFSJobCatalog(Config sysConfig, PathAlterationObserver observer, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled)
      throws IOException {
    super(Optional.of(LOGGER), parentMetricContext, instrumentationEnabled);
    this.sysConfig = sysConfig;
    ConfigAccessor cfgAccessor = new ConfigAccessor(this.sysConfig);

    this.jobConfDirPath = cfgAccessor.getJobConfDirPath();
    this.fs = cfgAccessor.getJobConfDirFileSystem();

    this.loader = new PullFileLoader(jobConfDirPath, jobConfDirPath.getFileSystem(new Configuration()),
        cfgAccessor.getJobConfigurationFileExtensions(),
        PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
    this.converter = new ImmutableFSJobCatalog.JobSpecConverter(this.jobConfDirPath, getInjectedExtension());

    long pollingInterval = cfgAccessor.getPollingInterval();

    if (pollingInterval == ConfigurationKeys.DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL) {
      this.pathAlterationDetector = null;
    }
    else {
      this.pathAlterationDetector = new PathAlterationObserverScheduler(pollingInterval);

      // If absent, the Optional object will be created automatically by addPathAlterationObserver
      Optional<PathAlterationObserver> observerOptional = Optional.fromNullable(observer);
      FSPathAlterationListenerAdaptor configFilelistener =
          new FSPathAlterationListenerAdaptor(this.jobConfDirPath, this.loader, this.sysConfig, this.listeners,
              this.converter);
      this.pathAlterationDetector.addPathAlterationObserver(configFilelistener, observerOptional, this.jobConfDirPath);
    }
  }

  @Override
  protected void startUp()
      throws IOException {
    super.startUp();

    if (this.pathAlterationDetector != null) {
      this.pathAlterationDetector.start();
    }
  }

  @Override
  protected void shutDown()
      throws IOException {
    try {
      if (this.pathAlterationDetector != null) {
        this.pathAlterationDetector.stop();
      }
    } catch (InterruptedException exc) {
      throw new RuntimeException("Failed to stop " + ImmutableFSJobCatalog.class.getName(), exc);
    }
    super.shutDown();
  }

  /**
   * Fetch all the job files under the jobConfDirPath
   * @return A collection of JobSpec
   */
  @Override
  public synchronized List<JobSpec> getJobs() {
    return Lists.transform(Lists.newArrayList(
        loader.loadPullFilesRecursively(loader.getRootDirectory(), this.sysConfig, shouldLoadGlobalConf())),
        this.converter);
  }

  /**
   * Fetch single job file based on its URI,
   * return null requested URI not existed
   * @param uri The relative Path to the target job configuration.
   * @return
   */
  @Override
  public synchronized JobSpec getJobSpec(URI uri)
      throws JobSpecNotFoundException {
    try {
      Path targetJobSpecFullPath = getPathForURI(this.jobConfDirPath, uri);
      return this.converter.apply(loader.loadPullFile(targetJobSpecFullPath, this.sysConfig, shouldLoadGlobalConf()));
    } catch (FileNotFoundException e) {
      throw new JobSpecNotFoundException(uri);
    } catch (IOException e) {
      throw new RuntimeException("IO exception thrown on loading single job configuration file:" + e.getMessage());
    }
  }

  /**
   * For immutable job catalog,
   * @return
   */
  public boolean shouldLoadGlobalConf() {
    return true;
  }

  /**
   *
   * @param jobConfDirPath The directory path for job cofniguration files.
   * @param uri Uri as the identifier of JobSpec
   * @return
   */
  protected Path getPathForURI(Path jobConfDirPath, URI uri) {
    return PathUtils.mergePaths(jobConfDirPath, new Path(uri));
  }

  protected Optional<String> getInjectedExtension() {
    return Optional.absent();
  }

  @Getter
  public static class ConfigAccessor {
    private final Config cfg;
    private final long pollingInterval;
    private final String jobConfDir;
    private final Path jobConfDirPath;
    private final FileSystem jobConfDirFileSystem;
    private final Set<String> JobConfigurationFileExtensions;

    public ConfigAccessor(Config cfg) {
      this.cfg = cfg;
      this.pollingInterval = this.cfg.hasPath(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY) ?
          this.cfg.getLong(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY)  :
          ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL;

      if (this.cfg.hasPath(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
        this.jobConfDir = this.cfg.getString(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY);
      }
      else if (this.cfg.hasPath(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
        File localJobConfigDir = new File(this.cfg.getString(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
        this.jobConfDir = "file://" + localJobConfigDir.getAbsolutePath();
      }
      else {
        throw new IllegalArgumentException("Expected " + ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY
            + " or " + ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY + " properties.");
      }
      this.jobConfDirPath = new Path(this.jobConfDir);
      try {
        this.jobConfDirFileSystem = this.jobConfDirPath.getFileSystem(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException("Unable to detect job config directory file system: " + e, e);
      }

      this.JobConfigurationFileExtensions = ImmutableSet.<String>copyOf(Splitter.on(",")
          .omitEmptyStrings()
          .trimResults()
          .split(getJobConfigurationFileExtensionsString()));
    }

    private String getJobConfigurationFileExtensionsString() {
      String propValue = this.cfg.hasPath(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY) ?
          this.cfg.getString(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY).toLowerCase() :
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS;
      return propValue;
    }
  }

  /**
   * Instance of this function is passed as a parameter into other transform function,
   * for converting Properties object into JobSpec object.
   */
  @AllArgsConstructor
  public static class JobSpecConverter implements Function<Config, JobSpec> {
    private final Path jobConfigDirPath;
    private final Optional<String> extensionToStrip;

    public URI computeURI(Path filePath) {
      // Make sure this is relative
      URI uri = PathUtils.relativizePath(filePath,
          jobConfigDirPath).toUri();
      if (this.extensionToStrip.isPresent()) {
        uri = PathUtils.removeExtension(new Path(uri), this.extensionToStrip.get()).toUri();
      }
      return uri;
    }

    @Nullable
    @Override
    public JobSpec apply(Config rawConfig) {

      URI jobConfigURI = computeURI(new Path(rawConfig.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)));

      // Points to noted:
      // 1.To ensure the transparency of JobCatalog, need to remove the addtional
      //   options that added through the materialization process.
      // 2. For files that created by user directly in the file system, there might not be
      //   version and description provided. Set them to default then, according to JobSpec constructor.

      String version;
      if (rawConfig.hasPath(VERSION_KEY_IN_JOBSPEC)) {
        version = rawConfig.getString(VERSION_KEY_IN_JOBSPEC);
      } else {
        // Set the version as default.
        version = "1";
      }

      String description;
      if (rawConfig.hasPath(DESCRIPTION_KEY_IN_JOBSPEC)) {
        description = rawConfig.getString(DESCRIPTION_KEY_IN_JOBSPEC);
      } else {
        // Set description as default.
        description = "Gobblin job " + jobConfigURI;
      }

      Config filteredConfig = rawConfig.withoutPath(FS_CATALOG_KEY_PREFIX);
      // The builder has null-checker. Leave the checking there.
      JobSpec.Builder builder = JobSpec.builder(jobConfigURI).withConfig(filteredConfig)
          .withDescription(description)
          .withVersion(version);

      if (rawConfig.hasPath(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
        try {
          builder.withTemplate(new URI(rawConfig.getString(ConfigurationKeys.JOB_TEMPLATE_PATH)));
        } catch (URISyntaxException e) {
          throw new RuntimeException("Bad job template URI " + e, e);
        }
      }

      return builder.build();
    }
  }
}
