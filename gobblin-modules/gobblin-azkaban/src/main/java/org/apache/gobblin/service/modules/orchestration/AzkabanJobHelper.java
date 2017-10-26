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
package org.apache.gobblin.service.modules.orchestration;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;


@Slf4j
public class AzkabanJobHelper {

  /***
   * Checks if an Azkaban project exists by name.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config that contains project name.
   * @return true if project exists else false.
   * @throws IOException
   */
  public static boolean isAzkabanJobPresent(String sessionId, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Checking if Azkaban project: " + azkabanProjectConfig.getAzkabanProjectName() + " exists");
    try {
      // NOTE: hacky way to determine if project already exists because Azkaban does not provides a way to
      // .. check if the project already exists or not
      boolean isPresent = StringUtils.isNotBlank(AzkabanAjaxAPIClient.getProjectId(sessionId, azkabanProjectConfig));
      log.info("Project exists: " + isPresent);

      return isPresent;
    } catch (IOException e) {
      // Project doesn't exists
      if (String.format("Project %s doesn't exist.", azkabanProjectConfig.getAzkabanProjectName())
          .equalsIgnoreCase(e.getMessage())) {
        log.info("Project does not exists.");
        return false;
      }
      // Project exists but with no read access to current user
      if ("Permission denied. Need READ access.".equalsIgnoreCase(e.getMessage())) {
        log.info("Project exists, but current user does not has READ access.");
        return true;
      }
      // Some other error
      log.error("Issue in checking if project is present", e);
      throw e;
    }
  }

  /***
   * Get Project Id by an Azkaban Project Name.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config that contains project Name.
   * @return Project Id.
   * @throws IOException
   */
  public static String getProjectId(String sessionId, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Getting project Id for project: " + azkabanProjectConfig.getAzkabanProjectName());
    String projectId = AzkabanAjaxAPIClient.getProjectId(sessionId, azkabanProjectConfig);
    log.info("Project id: " + projectId);

    return projectId;
  }

  /***
   * Create project on Azkaban based on Azkaban config. This includes preparing the zip file and uploading it to
   * Azkaban, setting permissions and schedule.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @return Project Id.
   * @throws IOException
   */
  public static String createAzkabanJob(String sessionId, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Creating Azkaban project for: " + azkabanProjectConfig.getAzkabanProjectName());

    // Create zip file
    String zipFilePath = createAzkabanJobZip(azkabanProjectConfig);
    log.info("Zip file path: " + zipFilePath);

    // Upload zip file to Azkaban
    String projectId = AzkabanAjaxAPIClient.createAzkabanProject(sessionId, zipFilePath, azkabanProjectConfig);
    log.info("Project Id: " + projectId);

    return projectId;
  }

  /***
   * Delete project on Azkaban based on Azkaban config.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @throws IOException
   */
  public static void deleteAzkabanJob(String sessionId, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Deleting Azkaban project for: " + azkabanProjectConfig.getAzkabanProjectName());

    // Delete project
    AzkabanAjaxAPIClient.deleteAzkabanProject(sessionId, azkabanProjectConfig);
  }

  /***
   * Replace project on Azkaban based on Azkaban config. This includes preparing the zip file and uploading it to
   * Azkaban, setting permissions and schedule.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @return Project Id.
   * @throws IOException
   */
  public static String replaceAzkabanJob(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig) throws IOException {
    log.info("Replacing zip for Azkaban project: " + azkabanProjectConfig.getAzkabanProjectName());

    // Create zip file
    String zipFilePath = createAzkabanJobZip(azkabanProjectConfig);
    log.info("Zip file path: " + zipFilePath);

    // Replace the zip file on Azkaban
    String projectId = AzkabanAjaxAPIClient.replaceAzkabanProject(sessionId, zipFilePath, azkabanProjectConfig);
    log.info("Project Id: " + projectId);

    return projectId;
  }

  /***
   * Schedule an already created Azkaban project.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config that contains schedule information.
   * @throws IOException
   */
  public static void scheduleJob(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Scheduling Azkaban project: " + azkabanProjectConfig.getAzkabanProjectName());
    AzkabanAjaxAPIClient.scheduleAzkabanProject(sessionId, azkabanProjectId, azkabanProjectConfig);
  }

  /***
   * Change the schedule of an already created Azkaban project.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config that contains schedule information.
   * @throws IOException
   */
  public static void changeJobSchedule(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Changing schedule for Azkaban project: " + azkabanProjectConfig.getAzkabanProjectName());
    AzkabanAjaxAPIClient.scheduleAzkabanProject(sessionId, azkabanProjectId, azkabanProjectConfig);
  }

  /***
   * Execute an already created Azkaban project.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config that contains schedule information.
   * @throws IOException
   */
  public static void executeJob(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Executing Azkaban project: " + azkabanProjectConfig.getAzkabanProjectName());
    AzkabanAjaxAPIClient.executeAzkabanProject(sessionId, azkabanProjectId, azkabanProjectConfig);
  }

  /***
   * Create Azkaban project zip file.
   * @param azkabanProjectConfig Azkaban Project Config that contains information about what to include in
   *                             zip file.
   * @return Zip file path.
   * @throws IOException
   */
  private static String createAzkabanJobZip(AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    log.info("Creating Azkaban job zip file for project: " + azkabanProjectConfig.getAzkabanProjectName());
    String workDir = azkabanProjectConfig.getWorkDir();

    Optional<String> jarUrlTemplate = azkabanProjectConfig.getAzkabanZipJarUrlTemplate();
    Optional<List<String>> jarNames = azkabanProjectConfig.getAzkabanZipJarNames();
    Optional<String> jarVersion = azkabanProjectConfig.getAzkabanZipJarVersion();
    Optional<List<String>> additionalFiles = azkabanProjectConfig.getAzkabanZipAdditionalFiles();
    boolean failIfJarNotFound = azkabanProjectConfig.getFailIfJarNotFound();
    String jobFlowName = azkabanProjectConfig.getAzkabanProjectFlowName();
    String zipFilename = azkabanProjectConfig.getAzkabanProjectZipFilename();

    // Download the job jars
    List<File> filesToAdd = Lists.newArrayList();
    if (jarNames.isPresent() && jarUrlTemplate.isPresent() && jarVersion.isPresent()) {
      String urlTemplate = jarUrlTemplate.get();
      String version = jarVersion.get();
      for (String jarName : jarNames.get()) {
        String jobJarUrl = urlTemplate.replaceAll("<module-version>", version).replaceAll("<module-name>", jarName);
        log.info("Downloading job jar from: " + jobJarUrl + " to: " + workDir);
        File jobJarFile = null;
        try {
          jobJarFile = downloadAzkabanJobJar(workDir, jobJarUrl);
          filesToAdd.add(jobJarFile);
        } catch (IOException e) {
          if (failIfJarNotFound) {
            throw e;
          }
          log.warn("Could not download: " + jobJarFile);
        }
      }
    }

    // Download additional files
    if (additionalFiles.isPresent()) {
      List<String> files = additionalFiles.get();
      for (String fileName : files) {
        log.info("Downloading additional file from: " + fileName + " to: " + workDir);
        File additionalFile = null;
        try {
          additionalFile = downloadAzkabanJobJar(workDir, fileName);
          filesToAdd.add(additionalFile);
        } catch (IOException e) {
          if(failIfJarNotFound) {
            throw e;
          }
          log.warn("Could not download: " + additionalFile);
        }
      }
    }

    // Write the config files
    log.info("Writing Azkaban config files");
    File [] jobConfigFile = writeAzkabanConfigFiles(workDir, jobFlowName, azkabanProjectConfig);
    filesToAdd.add(jobConfigFile[0]);

    // Create the zip file
    log.info("Writing zip file");
    String zipfile = createZipFile(workDir, zipFilename, filesToAdd);
    log.info("Wrote zip file: " + zipfile);

    return zipfile;
  }

  private static String createZipFile(String directory, String zipFilename, List<File> filesToAdd)
      throws IOException {
    // Determine final zip file path
    String zipFilePath = String.format("%s/%s", directory, zipFilename);
    File zipFile = new File(zipFilePath);
    if (zipFile.exists()) {
      if (zipFile.delete()) {
        log.info("Zipfile existed and was deleted: " + zipFilePath);
      } else {
        log.warn("Zipfile exists but was not deleted: " + zipFilePath);
      }
    }

    // Create and add files to zip file
    addFilesToZip(zipFile, filesToAdd);

    return zipFilePath;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "OBL_UNSATISFIED_OBLIGATION",
      justification = "Lombok construct of @Cleanup is handing this, but not detected by FindBugs")
  private static void addFilesToZip(File zipFile, List<File> filesToAdd) throws IOException {
    try {
      @Cleanup
      OutputStream archiveStream = new FileOutputStream(zipFile);
      @Cleanup
      ArchiveOutputStream archive =
          new ArchiveStreamFactory().createArchiveOutputStream(ArchiveStreamFactory.ZIP, archiveStream);

      for (File fileToAdd : filesToAdd) {
        ZipArchiveEntry entry = new ZipArchiveEntry(fileToAdd.getName());
        archive.putArchiveEntry(entry);

        @Cleanup
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(fileToAdd));
        IOUtils.copy(input, archive);
        archive.closeArchiveEntry();
      }

      archive.finish();
    } catch (ArchiveException e) {
      throw new IOException("Issue with creating archive", e);
    }
  }

  private static File[] writeAzkabanConfigFiles(String workDir, String flowName, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    // Determine final config file path
    String jobFilePath = String.format("%s/%s.job", workDir, flowName);
    File jobFile = new File(jobFilePath);
    if (jobFile.exists()) {
      if (jobFile.delete()) {
        log.info("JobFile existed and was deleted: " + jobFilePath);
      } else {
        log.warn("JobFile exists but was not deleted: " + jobFilePath);
      }
    }

    StringBuilder propertyFileContent = new StringBuilder();
    for (Map.Entry entry : azkabanProjectConfig.getJobSpec().getConfigAsProperties().entrySet()) {
      propertyFileContent.append(String.format("%s=%s", entry.getKey(), entry.getValue())).append("\n");
    }

    // Write the job file
    FileUtils.writeStringToFile(jobFile, propertyFileContent.toString(), Charset.forName("UTF-8"),true);

    return new File[] {jobFile};
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "OBL_UNSATISFIED_OBLIGATION",
      justification = "Lombok construct of @Cleanup is handing this, but not detected by FindBugs")
  private static File downloadAzkabanJobJar(String workDir, String jobJarUrl)
      throws IOException {
    // Determine final jar file path
    String[] jobJarUrlParts = jobJarUrl.trim().split("/");
    String jobJarName = jobJarUrlParts[jobJarUrlParts.length-1];
    String jobJarFilePath = String.format("%s/%s", workDir, jobJarName);
    File jobJarFile = new File(jobJarFilePath);
    if (jobJarFile.exists()) {
      if (jobJarFile.delete()) {
      log.info("JobJarFilePath existed and was deleted: " + jobJarFilePath);
    } else {
        log.warn("JobJarFilePath exists but was not deleted: " + jobJarFilePath);
      }
    }

    // Create work directory if not already exists
    FileUtils.forceMkdir(new File(workDir));

    // Download jar file from artifactory
    @Cleanup InputStream jobJarInputStream = new URL(jobJarUrl).openStream();
    @Cleanup OutputStream jobJarOutputStream = new FileOutputStream(jobJarFile);
    IOUtils.copy(jobJarInputStream, jobJarOutputStream);

    // TODO: compare checksum

    return jobJarFile;
  }
}

