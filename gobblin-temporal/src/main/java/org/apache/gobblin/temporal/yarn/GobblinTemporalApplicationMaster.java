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

package org.apache.gobblin.temporal.yarn;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import lombok.Getter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.cluster.GobblinTemporalClusterManager;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.apache.gobblin.yarn.GobblinYarnLogSource;
import org.apache.gobblin.yarn.YarnContainerSecurityManager;
import org.apache.gobblin.yarn.YarnHelixUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * The Yarn ApplicationMaster class for Gobblin using Temporal.
 *
 * <p>
 *   This class runs the {@link YarnService} for all Yarn-related stuffs like ApplicationMaster registration
 *   and un-registration and Yarn container provisioning.
 * </p>
 *
 */
@Alpha
public class GobblinTemporalApplicationMaster extends GobblinTemporalClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalApplicationMaster.class);

  @Getter
  private final YarnService _yarnService;
  private LogCopier logCopier;

  public GobblinTemporalApplicationMaster(String applicationName, String applicationId, ContainerId containerId, Config config,
      YarnConfiguration yarnConfiguration) throws Exception {
    super(applicationName, applicationId, config.withValue(GobblinYarnConfigurationKeys.CONTAINER_NUM_KEY,
            ConfigValueFactory.fromAnyRef(YarnHelixUtils.getContainerNum(containerId.toString()))),
        Optional.<Path>absent());

    if (config.hasPath(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY)) {
      String containerLogDir = config.getString(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY);
      GobblinYarnLogSource gobblinYarnLogSource = new GobblinYarnLogSource();
      if (gobblinYarnLogSource.isLogSourcePresent()) {
        Path appWorkDir = PathUtils.combinePaths(containerLogDir, GobblinClusterUtils.getAppWorkDirPath(this.clusterName, this.applicationId), "AppMaster");
        logCopier = gobblinYarnLogSource.buildLogCopier(this.config, containerId.toString(), this.fs, appWorkDir);
        this.applicationLauncher.addService(logCopier);
      }
    }
    YarnHelixUtils.setYarnClassPath(config, yarnConfiguration);
    YarnHelixUtils.setAdditionalYarnClassPath(config, yarnConfiguration);
    this._yarnService = buildTemporalYarnService(this.config, applicationName, this.applicationId, yarnConfiguration, this.fs);
    this.applicationLauncher.addService(this._yarnService);

    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnContainerSecurityManager since security is enabled");
      this.applicationLauncher.addService(buildYarnContainerSecurityManager(this.config, this.fs));
    }

    // Add additional services
    List<String> serviceClassNames = ConfigUtils.getStringList(this.config,
        GobblinYarnConfigurationKeys.APP_MASTER_SERVICE_CLASSES);

    for (String serviceClassName : serviceClassNames) {
      Class<?> serviceClass = Class.forName(serviceClassName);
      this.applicationLauncher.addService((Service) GobblinConstructorUtils.invokeLongestConstructor(serviceClass, this));
    }

    // Register last so we only add the hook after construction succeeds; hook runs on JVM exit to clean writer dirs.
    registerWorkDirCleanupShutdownHook();
  }

  /**
   * Build the {@link YarnService} for the Application Master.
   */
  protected YarnService buildTemporalYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs)
      throws Exception {
    return new DynamicScalingYarnService(config, applicationName, applicationId, yarnConfiguration, fs, this.eventBus);
  }

  /**
   * Build the {@link YarnTemporalAppMasterSecurityManager} for the Application Master.
   */
  private YarnContainerSecurityManager buildYarnContainerSecurityManager(Config config, FileSystem fs) {
    return new YarnTemporalAppMasterSecurityManager(config, fs, this.eventBus, this.logCopier, this._yarnService);
  }

  /**
   * Registers a JVM shutdown hook that cleans up work directories on exit.
   * The cleanup happens in the following order:
   * 1. Load JobState to get the list of directories to clean
   * 2. Clean staging/output directories first (from WORK_DIR_PATHS_TO_DELETE if available)
   * 3. Clean work directory root last
   *
   * This ordering is critical because JobState is stored in the work directory, so we must
   * read it before deleting the work directory root.
   */
  private void registerWorkDirCleanupShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        cleanupWorkDirsFromConfig(this.config);
      } catch (Exception e) {
        LOGGER.debug("Shutdown hook: cleanup of work dirs failed", e);
      }
    }, "GobblinTemporalAM-WorkDirCleanup"));
  }

  /**
   * Cleans up work directories in the following order:
   * 1. Staging/output directories (using paths from JobState if available, otherwise from config)
   * 2. Work directory root
   *
   * Uses a fresh FileSystem so cleanup works when the caller's fs may be closed.
   * Package-private for unit testing.
   */
  static void cleanupWorkDirsFromConfig(Config config) {
    if (!ConfigUtils.getBoolean(config, GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED,
        Boolean.parseBoolean(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED))) {
      LOGGER.info("Work directory cleanup is disabled");
      return;
    }

    String fsUriStr = ConfigUtils.getString(config, ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);

    try {
      FileSystem fs = FileSystem.get(URI.create(fsUriStr), new org.apache.hadoop.conf.Configuration());
      try {
        // Step 1: Try to load JobState to get precise paths to clean
        JobState jobState = loadJobStateFromConfig(config, fs);

        if (jobState != null && jobState.contains(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE)) {
          // Use paths from JobState (most precise - collected during GenerateWorkUnits)
          Set<String> pathsToDelete = jobState.getPropAsSet(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE);
          LOGGER.info("Shutdown cleanup: deleting {} work directories from JobState", pathsToDelete.size());
          for (String pathStr : pathsToDelete) {
            Path path = new Path(pathStr);
            if (fs.exists(path)) {
              LOGGER.info("Shutdown cleanup: deleting work directory {}", path);
              HadoopUtils.deletePath(fs, path, true);
            }
          }
        } else {
          // Fallback: Clean staging/output dirs from config
          cleanupStagingAndOutputDirsFromConfig(config, fs);
        }

        // Step 2: Clean work directory root last (after reading JobState and cleaning staging/output)
        if (jobState != null) {
          Path workDirRoot = JobStateUtils.getWorkDirRoot(jobState);
          if (fs.exists(workDirRoot)) {
            LOGGER.info("Shutdown cleanup: deleting work directory root {}", workDirRoot);
            HadoopUtils.deletePath(fs, workDirRoot, true);
          }
        }
      } finally {
        fs.close();
      }
    } catch (Exception e) {
      LOGGER.debug("Shutdown cleanup of work dirs failed: {}", e.getMessage());
    }
  }

  /**
   * Attempts to load JobState from the work directory specified in config.
   * Returns null if JobState cannot be loaded.
   *
   * Note: We use a simple implementation here instead of Help.loadJobState() because:
   * - We're starting from Config, not from existing JobStateful objects
   * - This is error-tolerant fallback code in a shutdown hook
   * - We don't need caching or retry logic here
   */
  private static JobState loadJobStateFromConfig(Config config, FileSystem fs) {
    try {
      String jobName = ConfigUtils.getString(config, ConfigurationKeys.JOB_NAME_KEY, null);
      String jobId = ConfigUtils.getString(config, ConfigurationKeys.JOB_ID_KEY, null);
      String mrJobRootDir = ConfigUtils.getString(config, ConfigurationKeys.MR_JOB_ROOT_DIR_KEY, null);

      if (jobName == null || jobId == null || mrJobRootDir == null) {
        LOGGER.debug("Cannot load JobState: missing job name, ID, or MR root dir in config");
        return null;
      }

      // Construct path using same logic as JobStateUtils.getWorkDirRoot()
      Path workDirRoot = new Path(new Path(mrJobRootDir, jobName), jobId);
      Path jobStateFile = new Path(workDirRoot, "job.state");

      if (!fs.exists(jobStateFile)) {
        LOGGER.debug("JobState file does not exist at {}", jobStateFile);
        return null;
      }

      JobState jobState = new JobState();
      try (DataInputStream dis = new DataInputStream(fs.open(jobStateFile))) {
        jobState.readFields(dis);
        LOGGER.info("Successfully loaded JobState from {}", jobStateFile);
        return jobState;
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to load JobState: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Deletes writer.staging.dir and writer.output.dir from the given config.
   * Used as fallback when WORK_DIR_PATHS_TO_DELETE is not available in JobState.
   */
  private static void cleanupStagingAndOutputDirsFromConfig(Config config, FileSystem fs) {
    String stagingPathStr = ConfigUtils.getString(config, ConfigurationKeys.WRITER_STAGING_DIR, null);
    String outputPathStr = ConfigUtils.getString(config, ConfigurationKeys.WRITER_OUTPUT_DIR, null);

    if (stagingPathStr == null && outputPathStr == null) {
      LOGGER.debug("No staging or output directories configured for cleanup");
      return;
    }

    try {
      if (stagingPathStr != null && !stagingPathStr.isEmpty()) {
        Path stagingPath = new Path(stagingPathStr);
        if (fs.exists(stagingPath)) {
          LOGGER.info("Shutdown cleanup: deleting writer staging dir {}", stagingPath);
          HadoopUtils.deletePath(fs, stagingPath, true);
        }
      }
      if (outputPathStr != null && !outputPathStr.isEmpty()) {
        Path outputPath = new Path(outputPathStr);
        if (fs.exists(outputPath)) {
          LOGGER.info("Shutdown cleanup: deleting writer output dir {}", outputPath);
          HadoopUtils.deletePath(fs, outputPath, true);
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to clean staging/output dirs: {}", e.getMessage());
    }
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    options.addOption("d", GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME, true, "Yarn application id");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinTemporalApplicationMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) ||
          (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME))) {
        printUsage(options);
        System.exit(1);
      }

      //Because AM is restarted with the original AppSubmissionContext, it may have outdated delegation tokens.
      //So the refreshed tokens should be added into the container's UGI before any HDFS/Hive/RM access is performed.
      YarnHelixUtils.updateToken(GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinTemporalApplicationMaster.class,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE);

      LOGGER.info(JvmUtils.getJvmInputArguments());

      ContainerId containerId =
          ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));

      try (GobblinTemporalApplicationMaster applicationMaster = new GobblinTemporalApplicationMaster(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME),
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME), containerId,
          ConfigFactory.load(), new YarnConfiguration())) {

        applicationMaster.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}

