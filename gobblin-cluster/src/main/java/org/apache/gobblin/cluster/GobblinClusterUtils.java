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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
  static final String JAVA_TMP_DIR_KEY = "java.io.tmpdir";
  /**
   * This template will be resolved by replacing "VALUE" as the value that gobblin recognized.
   * For more details, check {@link GobblinClusterUtils#setSystemProperties(Config)}
   */
  private static final String GOBBLIN_CLUSTER_SYSTEM_PROPERTY_LIST_TEMPLATE
      = GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX + "systemPropertiesList.$VALUE";

  /**
   * This enum is used for specifying JVM options that Gobblin-Cluster will set whose value will need to be obtained
   * in JVM runtime.
   * e.g. YARN_CACHE will be used by Gobblin-on-YARN (an extension of Gobblin-Cluster) and resolved to an YARN-specific
   * temporary location internal to the application.
   *
   * Note that we could specify a couple of keys associated with the value, meaning the value should only be resolved
   * to associated keys but nothing else to avoid abusive usage. Users could also set resolved
   * {@link #GOBBLIN_CLUSTER_SYSTEM_PROPERTY_LIST_TEMPLATE} to expand default associated-key list.
   *
   * e.g. setting `gobblin.cluster.systemPropertiesList.YARN_CACHE` = [a,b] expands the associated-key list to
   * [java.io.tmpdir, a, b]. Only when a key is found in the associated-key list, then when you set
   * {@link GobblinClusterConfigurationKeys#GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX}.${keyName}=YARN_CACHE, will the
   * resolution for the -D${KeyName} = resolvedValue(YARN_CACHE) happen.
   */
  public enum JVM_ARG_VALUE_RESOLVER {
    YARN_CACHE {
      @Override
      public List<String> getAssociatedKeys() {
        return yarnCacheAssociatedKeys;
      }

      @Override
      public String getResolution() {
        //When keys like java.io.tmpdir is configured to "YARN_CACHE", it sets the tmp dir to the Yarn container's cache location.
        // This setting will only be useful when the cluster is deployed in Yarn mode.
        return System.getenv(ApplicationConstants.Environment.PWD.key());
      }
    };

    // Kept for backward-compatibility
    private static List<String> yarnCacheAssociatedKeys = ImmutableList.of(JAVA_TMP_DIR_KEY);

    // default associated key with the value.
    public abstract List<String> getAssociatedKeys() ;

    public abstract String getResolution();

    public static boolean contains(String value) {
      for (JVM_ARG_VALUE_RESOLVER v : JVM_ARG_VALUE_RESOLVER.values()) {
        if (v.name().equalsIgnoreCase(value)) {
          return true;
        }
      }
      return false;
    }
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
    Properties properties = ConfigUtils.configToProperties(ConfigUtils.getConfig(config,
        GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX, ConfigFactory.empty()));

    for (Map.Entry<Object, Object> entry: properties.entrySet()) {
      if (JVM_ARG_VALUE_RESOLVER.contains(entry.getValue().toString())) {
        JVM_ARG_VALUE_RESOLVER enumMember = JVM_ARG_VALUE_RESOLVER.valueOf(entry.getValue().toString());
        List<String> allowedKeys = new ArrayList<>(enumMember.getAssociatedKeys());
        allowedKeys.addAll(getAdditionalKeys(entry.getValue().toString(), config));

        if (allowedKeys.contains(entry.getKey().toString())) {
          log.info("Setting tmp directory to: {}", enumMember.getResolution());
          System.setProperty(entry.getKey().toString(), enumMember.getResolution());
          continue;
        } else {
          log.warn("String {} not being registered for dynamic JVM-arg resolution, "
              + "considering add it by setting extension key", entry.getKey());
        }
      }
      System.setProperty(entry.getKey().toString(), entry.getValue().toString());
    }
  }

  private static Collection<String> getAdditionalKeys(String value, Config config) {
    String resolvedKey = GOBBLIN_CLUSTER_SYSTEM_PROPERTY_LIST_TEMPLATE.replace("$VALUE", value);
    if (config.hasPath(resolvedKey)) {
      return StreamSupport.stream(
          Splitter.on(",").trimResults().omitEmptyStrings().split(config.getString(resolvedKey)).spliterator(), false
      ).collect(Collectors.toList());
    } else {
      return Lists.newArrayList();
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
