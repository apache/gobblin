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
package gobblin.aws;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinHelixJobScheduler;
import gobblin.cluster.JobConfigurationManager;
import gobblin.cluster.event.NewJobConfigArrivalEvent;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ExecutorsUtils;
import gobblin.util.SchedulerUtils;

import static gobblin.aws.GobblinAWSUtils.appendSlash;


/**
 * Class for managing AWS Gobblin job configurations.
 *
 * <p>
 *   This class reads all the job configuration at startup from S3
 *   and schedules a refresh to poll from S3 for any new job configurations.
 *   The jobs read are scheduled by the {@link GobblinHelixJobScheduler} by posting a
 *   {@link NewJobConfigArrivalEvent} for each job configuration file.
 * </p>
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class AWSJobConfigurationManager extends JobConfigurationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSJobConfigurationManager.class);

  private static final long DEFAULT_JOB_CONF_REFRESH_INTERVAL = 60;

  private Optional<String> jobConfS3Uri;
  private Map<String, Properties> jobConfFiles;

  private final long refreshIntervalInSeconds;

  private final ScheduledExecutorService fetchJobConfExecutor;

  public AWSJobConfigurationManager(EventBus eventBus, Config config) {
    super(eventBus, config);
    this.jobConfFiles = Maps.newHashMap();
    if (config.hasPath(GobblinAWSConfigurationKeys.JOB_CONF_REFRESH_INTERVAL)) {
      this.refreshIntervalInSeconds = config.getDuration(GobblinAWSConfigurationKeys.JOB_CONF_REFRESH_INTERVAL,
          TimeUnit.SECONDS);
    } else {
      this.refreshIntervalInSeconds = DEFAULT_JOB_CONF_REFRESH_INTERVAL;
    }

    this.fetchJobConfExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("FetchJobConfExecutor")));
  }

  private void fetchJobConfSettings() {
    this.jobConfDirPath =
        config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY) ? Optional
            .of(config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) : Optional.<String>absent();
    this.jobConfS3Uri =
        config.hasPath(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY) ? Optional
            .of(config.getString(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY)) : Optional.<String>absent();
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + AWSJobConfigurationManager.class.getSimpleName());

    LOGGER.info(String.format("Scheduling the job configuration refresh task with an interval of %d second(s)",
            this.refreshIntervalInSeconds));

    // Schedule the job config fetch task
    this.fetchJobConfExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          fetchJobConf();
        } catch (IOException | ConfigurationException e) {
          LOGGER.error("Failed to fetch job configurations", e);
          throw new RuntimeException("Failed to fetch job configurations", e);
        }
      }
    }, 0, this.refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  private void fetchJobConf()
      throws IOException, ConfigurationException {
    // Refresh job config pull details from config
    fetchJobConfSettings();

    // TODO: Eventually when config store supports job files as well
    // .. we can replace this logic with config store
    if (this.jobConfS3Uri.isPresent() && this.jobConfDirPath.isPresent()) {

      // Download the zip file
      final String zipFile = appendSlash(this.jobConfDirPath.get()) +
          StringUtils.substringAfterLast(this.jobConfS3Uri.get(), File.separator);
      LOGGER.debug("Downloading to zip: " + zipFile + " from uri: " + this.jobConfS3Uri.get());

      FileUtils.copyURLToFile(new URL(this.jobConfS3Uri.get()), new File(zipFile));
      final String extractedPullFilesPath = appendSlash(this.jobConfDirPath.get()) + "files";

      // Extract the zip file
      LOGGER.debug("Extracting to directory: " + extractedPullFilesPath + " from zip: " + zipFile);
      unzipArchive(zipFile, new File(extractedPullFilesPath));

      // Load all new job configurations
      // TODO: Currently new and updated jobs are handled, we should un-schedule deleted jobs as well
      final File jobConfigDir = new File(extractedPullFilesPath);
      if (jobConfigDir.exists()) {
        LOGGER.info("Loading job configurations from " + jobConfigDir);
        final Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, jobConfigDir.getAbsolutePath());

        final List<Properties> jobConfigs = SchedulerUtils.loadGenericJobConfigs(properties);
        LOGGER.info("Loaded " + jobConfigs.size() + " job configuration(s)");
        for (Properties config : jobConfigs) {
          LOGGER.debug("Config value: " + config);

          // If new config or existing config got updated, then post new job config arrival event
          final String jobConfigPathIdentifier = config.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
          if (!jobConfFiles.containsKey(jobConfigPathIdentifier)) {
            jobConfFiles.put(jobConfigPathIdentifier, config);

            postNewJobConfigArrival(config.getProperty(ConfigurationKeys.JOB_NAME_KEY), config);
            LOGGER.info("New config arrived for job: " + jobConfigPathIdentifier);
          } else if (!config.equals(jobConfFiles.get(jobConfigPathIdentifier))) {
            jobConfFiles.put(jobConfigPathIdentifier, config);

            postNewJobConfigArrival(config.getProperty(ConfigurationKeys.JOB_NAME_KEY), config);
            LOGGER.info("Config updated for job: " + jobConfigPathIdentifier);
          } else {
            LOGGER.info("Config not changed for job: " + jobConfigPathIdentifier);
          }
        }
      } else {
        LOGGER.warn("Job configuration directory " + jobConfigDir + " not found");
      }
    }
  }

  /***
   * Unzip a zip archive
   * @param file Zip file to unarchive
   * @param outputDir Output directory for the unarchived file
   * @throws IOException If any issue occurs in unzipping the file
   */
  public void unzipArchive(String file, File outputDir)
      throws IOException {

    try (ZipFile zipFile = new ZipFile(file)) {

      final Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        final ZipEntry entry = entries.nextElement();
        final File entryDestination = new File(outputDir, entry.getName());

        if (entry.isDirectory()) {
          // If entry is directory, create directory
          if (!entryDestination.mkdirs() && !entryDestination.exists()) {
            throw new IOException("Could not create directory: " + entryDestination
                + " while un-archiving zip: " + file);
          }
        } else {
          // Create parent dirs if required
          if (!entryDestination.getParentFile().mkdirs() && !entryDestination.getParentFile().exists()) {
            throw new IOException("Could not create parent directory for: " + entryDestination
                + " while un-archiving zip: " + file);
          }

          // Extract and save the conf file
          InputStream in = null;
          OutputStream out = null;
          try {
            in = zipFile.getInputStream(entry);
            out = new FileOutputStream(entryDestination);
            IOUtils.copy(in, out);
          } finally {
            if (null != in)
              IOUtils.closeQuietly(in);
            if (null != out)
              IOUtils.closeQuietly(out);
          }
        }
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    GobblinAWSUtils.shutdownExecutorService(this.getClass(), this.fetchJobConfExecutor, LOGGER);
  }
}
