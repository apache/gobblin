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
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
import gobblin.cluster.GobblinHelixJobScheduler;
import gobblin.cluster.JobConfigurationManager;
import gobblin.cluster.event.NewJobConfigArrivalEvent;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.SchedulerUtils;

/**
 * A class for managing AWS Gobblin job configurations.
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

  private final Optional<String> jobConfS3Uri;
  private final Map<String, Properties> jobConfFiles;

  public AWSJobConfigurationManager(EventBus eventBus, Config config) {
    super(eventBus, config);

    this.jobConfFiles = Maps.newHashMap();
    this.jobConfS3Uri =
        config.hasPath(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY) ? Optional
            .of(config.getString(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY)) : Optional.<String>absent();
  }

  @Override
  protected void startUp() throws Exception {
    // TODO: Eventually when config store supports job files as well
    // .. we can replace this logic with config store
    if (this.jobConfS3Uri.isPresent() && this.jobConfDirPath.isPresent()) {

      // Download the zip file
      FileUtils.copyURLToFile(new URL(this.jobConfS3Uri.get()), new File(this.jobConfDirPath.get()));
      String zipFile = appendSlash(this.jobConfDirPath.get()) +
          StringUtils.substringAfterLast(this.jobConfS3Uri.get(), File.separator);
      String extractedPullFilesPath = appendSlash(this.jobConfDirPath.get()) + "files";

      // Extract the zip file
      unzipArchive(zipFile, new File(extractedPullFilesPath));

      // Load all new job configurations
      File jobConfigDir = new File(extractedPullFilesPath);
      if (jobConfigDir.exists()) {
        LOGGER.info("Loading job configurations from " + jobConfigDir);
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, jobConfigDir.getAbsolutePath());
        List<Properties> jobConfigs = SchedulerUtils.loadJobConfigs(properties);
        LOGGER.info("Loaded " + jobConfigs.size() + " job configuration(s)");
        for (Properties config : jobConfigs) {
          // If new config or existing config got updated, then post new job config arrival event
          String jobConfigPathIdentifier = config.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
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

  public void unzipArchive(String file, File outputDir)
      throws IOException {

    try (ZipFile zipFile = new ZipFile(file)) {

      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        File entryDestination = new File(outputDir, entry.getName());

        if (entry.isDirectory()) {
          // If entry is directory, create directory
          entryDestination.mkdirs();
        } else {
          // Create parent dirs if required
          entryDestination.getParentFile().mkdirs();

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
    // Nothing to do
  }

  private String appendSlash(String value) {
    if (value.endsWith("/")) {
      return value;
    }
    return value + "/";
  }
}
