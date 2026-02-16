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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;


/**
 * A utility class for Gobblin on Yarn.
 *
 * @author Yinan Li
 */
public class YarnHelixUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(YarnHelixUtils.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private static final Splitter ZIP_SPLITTER = Splitter.on('#').omitEmptyStrings().trimResults();

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
    addFileAsLocalResource(fs, destFilePath, resourceType, resourceMap, destFilePath.getName());
  }


  public static void addFileAsLocalResource(FileSystem fs, Path destFilePath, LocalResourceType resourceType,
      Map<String, LocalResource> resourceMap, String resourceName) throws IOException {
    LocalResource fileResource = Records.newRecord(LocalResource.class);
    FileStatus fileStatus = fs.getFileStatus(destFilePath);
    fileResource.setResource(ConverterUtils.getYarnUrlFromPath(destFilePath));
    fileResource.setSize(fileStatus.getLen());
    fileResource.setTimestamp(fileStatus.getModificationTime());
    fileResource.setType(resourceType);
    fileResource.setVisibility(LocalResourceVisibility.APPLICATION);
    resourceMap.put(resourceName, fileResource);
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

  public static void setYarnClassPath(Config config, Configuration yarnConfiguration) {
    if (!ConfigUtils.emptyIfNotPresent(config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_CLASSPATHS).equals(
        StringUtils.EMPTY)){
      yarnConfiguration.setStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, config.getString(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CLASSPATHS));
    }
  }

  /**
   * Calculate the path of a jar cache on HDFS, which is retained on a semi-monthly basis (twice per month).
   * directory lock threshold. Each month is split into two periods:
   * - Period 1: Days 1-15 (suffix: yyyy-MM-1)
   * - Period 2: Days 16-end (suffix: yyyy-MM-2)
   *
   * Should be used in conjunction with {@link #retainKLatestJarCachePaths(Path, int, FileSystem)} to clean up the cache on a periodic basis.
   *
   * @param config the configuration
   * @param fs the filesystem to use for validation
   * @return the semi-monthly jar cache path
   * @throws IOException if filesystem operations fail
   */
  public static Path calculatePerMonthJarCachePath(Config config, FileSystem fs) throws IOException {
    // Use JarCachePathResolver to resolve the base jar cache directory
    Path baseCacheDir = JarCachePathResolver.resolveJarCachePath(config, fs);

    // Calculate semi-monthly suffix based on day of month
    long startTime = config.getLong(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY);
    Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(startTime);

    String yearMonth = new SimpleDateFormat("yyyy-MM").format(cal.getTime());
    int dayOfMonth = cal.get(java.util.Calendar.DAY_OF_MONTH);

    // Partition by 15th of month: days 1-15 = period 1, days 16-end = period 2
    String periodSuffix = dayOfMonth <= 15 ? "1" : "2";
    String cacheSuffix = yearMonth + "-" + periodSuffix;

    return new Path(baseCacheDir, cacheSuffix);
  }

  /**
   * Retain the latest k jar cache paths that are children of the parent cache path.
   * Handles both old monthly format (yyyy-MM) and new semi-monthly format (yyyy-MM-1, yyyy-MM-2).
   * During migration, old monthly directories are treated as belonging to the first half of the month
   * for sorting purposes (equivalent to yyyy-MM-1), ensuring they are cleaned up before newer semi-monthly periods.
   *
   * @param parentCachePath the parent directory containing jar cache subdirectories
   * @param k the number of latest jar cache paths to retain
   * @param fs the filesystem
   * @return true if all deletes were successful, false otherwise
   * @throws IOException if filesystem operations fail
   */
  public static boolean retainKLatestJarCachePaths(Path parentCachePath, int k, FileSystem fs) throws IOException {
    if (!fs.exists(parentCachePath)) {
      return true;
    }

    // Get all jar cache directories
    FileStatus[] allDirs = fs.listStatus(parentCachePath);

    // Sort by normalized path name for proper chronological ordering
    // Old format (yyyy-MM) is treated as yyyy-MM-1 for sorting
    List<FileStatus> jarDirs = Arrays.stream(allDirs)
        .sorted((a, b) -> {
          String nameA = normalizeDirectoryNameForSorting(a.getPath().getName());
          String nameB = normalizeDirectoryNameForSorting(b.getPath().getName());
          return nameA.compareTo(nameB);
        })
        .collect(Collectors.toList());

    // Delete oldest directories, keeping k latest
    boolean deletesSuccessful = true;
    for (int i = 0; i < jarDirs.size() - k; i++) {
      Path toDelete = jarDirs.get(i).getPath();
      LOGGER.info("Deleting old jar cache directory: {}", toDelete);
      deletesSuccessful &= fs.delete(toDelete, true);
    }

    return deletesSuccessful;
  }

  /**
   * Normalizes directory names for sorting to handle migration from monthly to semi-monthly format.
   * Converts old monthly format (yyyy-MM) to yyyy-MM-1 for consistent chronological sorting.
   * This ensures old directories are cleaned up before newer semi-monthly periods in the same month.
   *
   * @param dirName the directory name (e.g., "2024-09" or "2024-09-1")
   * @return normalized name for sorting (e.g., "2024-09-1" or "2024-09-2")
   */
  @VisibleForTesting
  static String normalizeDirectoryNameForSorting(String dirName) {
    // Pattern: yyyy-MM or yyyy-MM-{1,2}
    // If it's old format (yyyy-MM without period suffix), treat as yyyy-MM-1
    if (dirName.matches("\\d{4}-\\d{2}$")) {
      // Old monthly format - treat as first half of month for aggressive cleanup
      return dirName + "-1";
    }
    // Already in new format (yyyy-MM-1 or yyyy-MM-2) or unrecognized format
    return dirName;
  }


  public static Set<String> getAppLibJarList(Config config) {
    Set<String> libAppJars = new HashSet<>(Arrays.asList(
        ConfigUtils.getString(config, GobblinYarnConfigurationKeys.YARN_APPLICATION_LIB_JAR_LIST, "").split(",")));
    Set<String> containerJars = new HashSet<>(Arrays.asList(
        ConfigUtils.getString(config, GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY, "").split(",")));
    libAppJars.addAll(containerJars);
    return Sets.filter(libAppJars, s -> !s.isEmpty());
  }

  public static void addRemoteFilesToLocalResources(String hdfsFileList, Map<String, LocalResource> resourceMap, Configuration yarnConfiguration) throws IOException {
    for (String hdfsFilePath : SPLITTER.split(hdfsFileList)) {
      Path srcFilePath = new Path(hdfsFilePath);
      YarnHelixUtils.addFileAsLocalResource(
          srcFilePath.getFileSystem(yarnConfiguration), srcFilePath, LocalResourceType.FILE, resourceMap);
    }
  }

  public static void addRemoteZipsToLocalResources(String hdfsFileList, Map<String, LocalResource> resourceMap, Configuration yarnConfiguration)
      throws IOException {
    for (String zipFileWithName : SPLITTER.split(hdfsFileList)) {
      Iterator<String> zipFileAndName =  ZIP_SPLITTER.split(zipFileWithName).iterator();
      Path srcFilePath = new Path(zipFileAndName.next());
      try {
        YarnHelixUtils.addFileAsLocalResource(srcFilePath.getFileSystem(yarnConfiguration), srcFilePath, LocalResourceType.ARCHIVE,
            resourceMap, zipFileAndName.next());
      } catch (Exception e) {
        throw new IOException(String.format("Fail to extract %s as local resources, maybe a wrong pattern, "
            + "correct pattern should be {zipPath}#{targetUnzippedName}", zipFileAndName), e);
      }
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

  /**
   * Find the helix tag for the newly allocated container. The tag should align with {@link YarnContainerRequestBundle},
   * so that the correct resource can be allocated to helix workflow that has specific resource requirement.
   * @param container newly allocated container
   * @param helixTagAllocatedContainerCount current container count for each helix tag
   * @param requestedYarnContainer yarn container request specify the desired state
   * @return helix tag that this container should be assigned with, if null means need to use the default
   */
  public static String findHelixTagForContainer(Container container,
      Map<String, AtomicInteger> helixTagAllocatedContainerCount, YarnContainerRequestBundle requestedYarnContainer) {
    String foundTag = null;
    if(requestedYarnContainer != null && requestedYarnContainer.getResourceHelixTagMap().containsKey(container.getResource().toString())) {
      for (String tag : requestedYarnContainer.getResourceHelixTagMap().get(container.getResource().toString())) {
        int desiredCount = requestedYarnContainer.getHelixTagContainerCountMap().get(tag);
        helixTagAllocatedContainerCount.putIfAbsent(tag, new AtomicInteger(0));
        int allocatedCount = helixTagAllocatedContainerCount.get(tag).get();
        foundTag = tag;
        if(allocatedCount < desiredCount) {
          return foundTag;
        }
      }
    }
    return foundTag;
  }
}
