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

package org.apache.gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;


/**
 * A utility class for Gobblin on Yarn.
 *
 * @author Yinan Li
 */
public class YarnHelixUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(YarnHelixUtils.class);

  /**
   * Write a {@link Token} to a given file.
   *
   * @param tokenFilePath the token file path
   * @param credentials all tokens of this credentials to be written to given file
   * @param configuration a {@link Configuration} object carrying Hadoop configuration properties
   * @throws IOException
   */
  public static void writeTokenToFile(Path tokenFilePath, Credentials credentials, Configuration configuration) throws IOException {
    if(credentials == null) {
      LOGGER.warn("got empty credentials, creating default one as new.");
      credentials = new Credentials();
    }
    LOGGER.debug(String.format("Writing all tokens %s to file %s",  credentials.getAllTokens(), tokenFilePath));
    credentials.writeTokenStorageFile(tokenFilePath, configuration);
  }

  /**
   * Update {@link Token} with token file localized by NM.
   *
   * @param tokenFileName name of the token file
   * @throws IOException
   */
  public static void updateToken(String tokenFileName) throws IOException{
    LOGGER.info("reading token from file: "+ tokenFileName);
    URL tokenFileUrl = YarnHelixUtils.class.getClassLoader().getResource(tokenFileName);
    if (tokenFileUrl != null) {
      File tokenFile = new File(tokenFileUrl.getFile());
      if (tokenFile.exists()) {
        Credentials credentials = Credentials.readTokenStorageFile(tokenFile, new Configuration());
        for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
          LOGGER.info("updating " + token.getKind() + " " + token.getService());
        }
        UserGroupInformation.getCurrentUser().addCredentials(credentials);
      }
    }
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
  @SuppressWarnings("deprecation")
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
    String[] additionalClassPath = yarnConfiguration.getStrings(GobblinYarnConfigurationKeys.GOBBLIN_YARN_ADDITIONAL_CLASSPATHS);
    if (additionalClassPath != null) {
      for (String classpath : additionalClassPath) {
        Apps.addToEnvironment(
            environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(), classpath.trim());
      }
    }

    return environmentVariableMap;
  }

  public static void setAdditionalYarnClassPath(Config config, Configuration yarnConfiguration) {
    if (!ConfigUtils.emptyIfNotPresent(config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_ADDITIONAL_CLASSPATHS).equals(
        StringUtils.EMPTY)){
      yarnConfiguration.setStrings(GobblinYarnConfigurationKeys.GOBBLIN_YARN_ADDITIONAL_CLASSPATHS, config.getString(GobblinYarnConfigurationKeys.GOBBLIN_YARN_ADDITIONAL_CLASSPATHS));
    }
  }

  /**
   * Return the identifier of the containerId. The identifier is the substring in the containerId representing
   * the sequential number of the container.
   * @param containerId e.g. "container_e94_1567552810874_2132400_01_000001"
   * @return sequence number of the containerId e.g. "container-000001"
   */
  public static String getContainerNum(String containerId) {
    return "container-" + containerId.substring(containerId.lastIndexOf("_") + 1);
  }
}
