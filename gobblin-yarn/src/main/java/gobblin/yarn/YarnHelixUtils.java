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

package gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.tools.ClusterSetup;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import com.google.common.collect.Maps;


/**
 * A utility class for Gobblin on Yarn/Helix.
 *
 * @author ynli
 */
public class YarnHelixUtils {

  /**
   * Create a Helix cluster for the Gobblin Yarn application.
   *
   * @param zkConnectionString the ZooKeeper connection string
   * @param clusterName the Helix cluster name
   */
  public static void createGobblinYarnHelixCluster(String zkConnectionString, String clusterName) {
    ClusterSetup clusterSetup = new ClusterSetup(zkConnectionString);
    // Create the cluster and overwrite if it already exists
    clusterSetup.addCluster(clusterName, true);
    // Helix 0.6.x requires a configuration property to have the form key=value.
    String autoJoinConfig = ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN + "=true";
    clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.CLUSTER, clusterName, autoJoinConfig);
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
   * Get a Helix partition ID from a given host name and a given Yarn {@link ContainerId}.
   *
   * @param hostName the given host name
   * @param containerId the given {@link ContainerId}
   * @return a Helix partition ID
   */
  public static String getParticipantId(String hostName, ContainerId containerId) {
    return getHelixInstanceName(hostName, containerId);
  }

  /**
   * Get a Helix instance name from a given host name and a given Yarn {@link ContainerId}.
   *
   * @param hostName the given host name
   * @param containerId the given {@link ContainerId}
   * @return a Helix instance name
   */
  public static String getHelixInstanceName(String hostName, ContainerId containerId) {
    return hostName + "_" + containerId.toString();
  }

  /**
   * Get the Yarn application root directory {@link Path}.
   *
   * @param fs a {@link FileSystem} instance on which {@link FileSystem#getHomeDirectory()} is called
   *           to get the home directory of the {@link FileSystem} of the application working directory
   * @param applicationName the Yarn application name
   * @return the Yarn application root directory {@link Path}
   */
  public static Path getAppRootDirPath(FileSystem fs, String applicationName) {
    return new Path(fs.getHomeDirectory(), applicationName);
  }

  /**
   * Get the Yarn application working directory {@link Path}.
   *
   * @param fs a {@link FileSystem} instance on which {@link FileSystem#getHomeDirectory()} is called
   *           to get the home directory of the {@link FileSystem} of the application working directory
   * @param applicationName the Yarn application name
   * @param applicationId the Yarn application ID in string form
   * @return the Yarn application working directory {@link Path}
   */
  public static Path getAppWorkDirPath(FileSystem fs, String applicationName, String applicationId) {
    return new Path(fs.getHomeDirectory(), applicationName + Path.SEPARATOR + applicationId);
  }

  /**
   * Convert a given {@link Config} instance to a {@link Properties} instance.
   *
   * @param config the given {@link Config} instance
   * @return a {@link Properties} instance
   */
  public static Properties configToProperties(Config config) {
    Properties properties = new Properties();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), config.getString(entry.getKey()));
    }

    return properties;
  }

  /**
   * Write a {@link Token} to a given file.
   *
   * @param token the token to write
   * @param tokenFilePath the token file path
   * @param configuration a {@link Configuration} object carrying Hadoop configuration properties
   * @throws IOException
   */
  public static void writeTokenToFile(Token<? extends TokenIdentifier> token, Path tokenFilePath,
      Configuration configuration) throws IOException {
    Credentials credentials = new Credentials();
    credentials.addToken(token.getService(), token);
    credentials.writeTokenStorageFile(tokenFilePath, configuration);
  }

  /**
   * Read a collection {@link Token}s from a given file.
   *
   * @param tokenFilePath the token file path
   * @param configuration a {@link Configuration} object carrying Hadoop configuration properties
   * @return a collection of {@link Token}s
   * @throws IOException
   */
  public static Collection<Token<? extends TokenIdentifier>> readTokensFromFile(Path tokenFilePath,
      Configuration configuration) throws IOException {
    return Credentials.readTokenStorageFile(tokenFilePath, configuration).getAllTokens();
  }

  /**
   * Add a file as a Yarn {@link org.apache.hadoop.yarn.api.records.LocalResource}.
   *
   * @param fs a {@link FileSystem} instance
   * @param destFilePath the destination file path
   * @param resourceType the {@link org.apache.hadoop.yarn.api.records.LocalResourceType} of the file
   * @param resourceMap a {@link Map} of file names to their corresponding
   *                    {@link org.apache.hadoop.yarn.api.records.LocalResource}s
   * @throws IOException if there's something wrong adding the file as a
   *                     {@link org.apache.hadoop.yarn.api.records.LocalResource}
   */
  public static void addFileAsLocalResource(FileSystem fs, Path destFilePath, LocalResourceType resourceType,
      Map<String, LocalResource> resourceMap) throws IOException {
    LocalResource fileResource = Records.newRecord(LocalResource.class);
    FileStatus fileStatus = fs.getFileStatus(destFilePath);
    fileResource.setResource(ConverterUtils.getYarnUrlFromPath(destFilePath));
    fileResource.setSize(fileStatus.getLen());
    fileResource.setTimestamp(fileStatus.getModificationTime());
    fileResource.setType(resourceType);
    fileResource.setVisibility(LocalResourceVisibility.APPLICATION);
    resourceMap.put(destFilePath.getName(), fileResource);
  }

  /**
   * Get environment variables in a {@link java.util.Map} used when launching a Yarn container.
   *
   * @param yarnConfiguration a Hadoop {@link Configuration} object carrying Hadoop/Yarn configuration properties
   * @return a {@link java.util.Map} storing environment variables used when launching a Yarn container
   */
  public static Map<String, String> getEnvironmentVariables(Configuration yarnConfiguration) {
    Map<String, String> environmentVariableMap = Maps.newHashMap();

    if (System.getenv().containsKey(ApplicationConstants.Environment.JAVA_HOME.key())) {
      Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.JAVA_HOME.key(),
          System.getenv(ApplicationConstants.Environment.JAVA_HOME.key()));
    }

    // Add jars/files in the working directory of the ApplicationMaster to the CLASSPATH
    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(),
        ApplicationConstants.Environment.PWD.$());
    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(),
        ApplicationConstants.Environment.PWD.$() + File.separator + "*");

    String[] classpaths = yarnConfiguration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
    if (classpaths != null) {
      for (String classpath : classpaths) {
        Apps.addToEnvironment(
            environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(), classpath.trim());
      }
    }

    return environmentVariableMap;
  }
}
