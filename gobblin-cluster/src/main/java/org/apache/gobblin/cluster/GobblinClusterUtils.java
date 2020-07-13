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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.gobblin.util.PathUtils;

@Alpha
@Slf4j
public class GobblinClusterUtils {
  public static final String JAVA_TMP_DIR_KEY = "java.io.tmpdir";

  public enum TMP_DIR {
    YARN_CACHE
  }

  /**
   * Get the name of the current host.
   *
   * @return the name of the current host
   * @throws UnknownHostException if the host name is unknown
   */
  public static String getHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  /**
   * Get the application working directory {@link Path}.
   *
   * @param fs a {@link FileSystem} instance on which {@link FileSystem#getHomeDirectory()} is called
   *           to get the home directory of the {@link FileSystem} of the application working directory
   * @param applicationName the application name
   * @param applicationId the application ID in string form
   * @return the cluster application working directory {@link Path}
   */
  public static Path getAppWorkDirPathFromConfig(Config config, FileSystem fs,
      String applicationName, String applicationId) {
    if (config.hasPath(GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR)) {
      return new Path(new Path(fs.getUri()), PathUtils.combinePaths(config.getString(GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR),
          getAppWorkDirPath(applicationName, applicationId)));
    }
    return new Path(fs.getHomeDirectory(), getAppWorkDirPath(applicationName, applicationId));
  }

  /**
   * Get the application working directory {@link String}.
   *
   * @param applicationName the application name
   * @param applicationId the application ID in string form
   * @return the cluster application working directory {@link String}
   */
  public static String getAppWorkDirPath(String applicationName, String applicationId) {
    return applicationName + Path.SEPARATOR + applicationId;
  }

  /**
   * Generate the path to the job.state file
   * @param usingStateStore is a state store being used to store the job.state content
   * @param appWorkPath work directory
   * @param jobId job id
   * @return a {@link Path} referring to the job.state
   */
  public static Path getJobStateFilePath(boolean usingStateStore, Path appWorkPath, String jobId) {
    final Path jobStateFilePath;

    // the state store uses a path of the form workdir/_jobstate/job_id/job_id.job.state while old method stores the file
    // in the app work dir.
    if (usingStateStore) {
      jobStateFilePath = new Path(appWorkPath, GobblinClusterConfigurationKeys.JOB_STATE_DIR_NAME
          + Path.SEPARATOR + jobId + Path.SEPARATOR + jobId + "."
          + AbstractJobLauncher.JOB_STATE_FILE_NAME);

    } else {
      jobStateFilePath = new Path(appWorkPath, jobId + "." + AbstractJobLauncher.JOB_STATE_FILE_NAME);
    }

    log.info("job state file path: " + jobStateFilePath);

    return jobStateFilePath;
  }

  /**
   * Set the system properties from the input {@link Config} instance
   * @param config
   */
  public static void setSystemProperties(Config config) {
    Properties properties = ConfigUtils.configToProperties(ConfigUtils.getConfig(config, GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX,
        ConfigFactory.empty()));

    for (Map.Entry<Object, Object> entry: properties.entrySet()) {
      if (entry.getKey().toString().equals(JAVA_TMP_DIR_KEY)) {
        if (entry.getValue().toString().equalsIgnoreCase(TMP_DIR.YARN_CACHE.toString())) {
          //When java.io.tmpdir is configured to "YARN_CACHE", it sets the tmp dir to the Yarn container's cache location.
          // This setting will only be useful when the cluster is deployed in Yarn mode.
          log.info("Setting tmp directory to: {}", System.getenv(ApplicationConstants.Environment.PWD.key()));
          System.setProperty(entry.getKey().toString(), System.getenv(ApplicationConstants.Environment.PWD.key()));
          continue;
        }
      }
      System.setProperty(entry.getKey().toString(), entry.getValue().toString());
    }
  }

  /**
   * Get the dynamic config from a {@link DynamicConfigGenerator}
   * @param config input config
   * @return  the dynamic config
   */
  public static Config getDynamicConfig(Config config) {
    // load dynamic configuration and add them to the job properties
    DynamicConfigGenerator dynamicConfigGenerator =
        DynamicConfigGeneratorFactory.createDynamicConfigGenerator(config);
    Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(config);

    return dynamicConfig;
  }

  /**
   * Add dynamic config with higher precedence to the input config
   * @param config input config
   * @return a config combining the input config with the dynamic config
   */
  public static Config addDynamicConfig(Config config) {
    return getDynamicConfig(config).withFallback(config);
  }

  /**
   * A utility method to construct a {@link FileSystem} object with the configured Hadoop overrides provided as part of
   * the cluster configuration.
   * @param config
   * @param conf
   * @return a {@link FileSystem} object that is instantiated with the appropriated Hadoop config overrides.
   * @throws IOException
   */
  public static FileSystem buildFileSystem(Config config, Configuration conf)
      throws IOException {
    Config hadoopOverrides = ConfigUtils.getConfigOrEmpty(config, GobblinClusterConfigurationKeys.HADOOP_CONFIG_OVERRIDES_PREFIX);

    //Add any Hadoop-specific overrides into the Configuration object
    JobConfigurationUtils.putPropertiesIntoConfiguration(ConfigUtils.configToProperties(hadoopOverrides), conf);
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), conf)
        : FileSystem.get(conf);
  }
}
