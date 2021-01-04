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

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



/**
 * This test is disabled by default because it assumes the Azkaban-solo-server is setup on localhost:8081.
 *
 * Please check https://azkaban.github.io/azkaban/docs/latest/ for how to setup Azkaban-solo-server.
 */
@Slf4j
@Test(enabled = false)
public class AzkabanClientTest {
  private AzkabanClient client = null;
  private long sessionExpireInMin = 1;

  String projectName;
  String description;

  @BeforeClass
  public void setup() throws Exception {
    Config azkConfig = ConfigFactory.load("local-azkaban-service.conf");
    String userName = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_USERNAME_KEY);
    String password = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_PASSWORD_KEY);
    String url = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_SERVER_URL_KEY);
    this.client = AzkabanClient.builder()
        .username(userName)
        .password(password)
        .url(url)
        .sessionExpireInMin(sessionExpireInMin)
        .build();
  }

  @BeforeMethod
  public void testSetup() {
    projectName = "test-project-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 4);
    description = "This is test project.";
  }

  @AfterMethod
  public void testCleanup() throws AzkabanClientException {
    this.client.deleteProject(projectName);
  }

  @AfterClass
  public void cleanup() throws IOException {
    this.client.close();
  }

  private void ensureProjectExist(String projectName, String description) throws AzkabanClientException {
    this.client.createProject(projectName, description);
  }

  public void testFetchLog() throws Exception {
    String flowName = "test-exec-flow";
    String jobId = "test-exec-flow";

    ensureProjectExist(projectName, description);
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    AzkabanExecuteFlowStatus execStatus = this.client.executeFlow(projectName, flowName, Maps.newHashMap());
    String execId = execStatus.getResponse().getExecId();

    ByteArrayOutputStream logStream = null;

    // Logs are not instantly available. Retrying several times until the job has started, and logs are present.
    int maxTries = 10;
    for (int i = 0; i < maxTries; i++) {
      logStream = new ByteArrayOutputStream();

      Thread.sleep(1000);
      try {
        this.client.fetchExecutionLog(execId, jobId, 0, 100000000, logStream);
        break;
      } catch (Exception ex) {
        if (i == maxTries - 1) {
          throw ex;
        }
      }
    }

    Assert.assertTrue(logStream.size() > 0);
  }

  public void testProjectCreateAndDelete() throws AzkabanClientException {
    this.client.createProject(projectName, description);
    this.client.deleteProject(projectName);
  }

  public void testProjectExistenceCheck() throws AzkabanClientException {
    Assert.assertFalse(this.client.projectExists(projectName));

    this.client.createProject(projectName, description);
    Assert.assertTrue(this.client.projectExists(projectName));

    this.client.deleteProject(projectName);
    Assert.assertFalse(this.client.projectExists(projectName));
  }

  public void testUploadZip() throws IOException {
    String flowName = "test-upload";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    // upload Zip to an non-existed project
    try {
      this.client.uploadProjectZip("Non-existed-project", zipFile);
      Assert.fail();
    } catch (Exception e) {
      log.info("Expected exception " + e.toString());
    }
  }

  public void testExecuteFlow() throws IOException {
    String flowName = "test-exec-flow";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlow(projectName, flowName, Maps.newHashMap());
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  public void testExecuteFlowWithParams() throws IOException {
    String flowName = "test-exec-flow-param";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    Map<String, String> flowParams = Maps.newHashMap();
    flowParams.put("gobblin.source", "DummySource");
    flowParams.put("gobblin.dataset.pattern", "/data/tracking/MessageActionEvent/hourly/*/*/*/*");

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlow(projectName, flowName, flowParams);
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  public void testExecuteFlowWithOptions() throws IOException {
    String flowName = "test-exec-flow-options";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    Map<String, String> flowOptions = Maps.newHashMap();

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlowWithOptions(projectName, flowName, flowOptions, Maps.newHashMap());
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  public void testFetchFlowExecution() throws Exception {
    String flowName = "test-fetch-flow-executions";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    Map<String, String> flowOptions = Maps.newHashMap();

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlowWithOptions(projectName, flowName, flowOptions, Maps.newHashMap());
    log.info("Execid: {}", execStatus.getResponse().execId);

    // wait for the job started and failed
    Thread.sleep(3000);

    // job should fail
    AzkabanFetchExecuteFlowStatus fetchExecuteFlowStatus = this.client.fetchFlowExecution(execStatus.getResponse().execId);
    for (Map.Entry<String, String> entry : fetchExecuteFlowStatus.getResponse().getMap().entrySet()) {
      log.info(entry.getKey() + " -> " + entry.getValue());
    }
  }

  @Test(enabled = false)
  public void testSessionExpiration() throws Exception {
    Thread.sleep(sessionExpireInMin * 60 * 1000);
    ensureProjectExist(projectName, description);
  }

  public void testGettingProjectFlows() throws IOException {
    String flowName = "test-exec-flow";

    ensureProjectExist(projectName, description);

    AzkabanProjectFlowsStatus status = this.client.fetchProjectFlows(projectName);
    Assert.assertTrue(status.getResponse().getFlows().isEmpty());

    File zipFile = createAzkabanZip(flowName);
    this.client.uploadProjectZip(projectName, zipFile);

    status = this.client.fetchProjectFlows(projectName);
    List<AzkabanProjectFlowsStatus.Flow> flows = status.getResponse().getFlows();
    Assert.assertEquals(1, flows.size());
    Assert.assertEquals(flowName, flows.get(0).flowId);
  }

  private File createAzkabanZip(String flowName) throws IOException {
    Properties jobProps = new Properties();
    jobProps.load(this.getClass().getClassLoader().
        getResourceAsStream("azkakaban-job-basic.properties"));

    String basePath = "/tmp/testAzkabanZip";
    FileUtils.deleteDirectory(new File(basePath));

    // create testAzkabanZip/test dir
    File jobDir = new File(basePath, flowName);
    Assert.assertTrue(jobDir.mkdirs());

    // create testAzkabanZip/test/test.job
    File jobFile = new File(jobDir,flowName + ".job");
    OutputStream jobOut = new FileOutputStream(jobFile);
    jobProps.store(jobOut, "Writing a test job file.");

    // create testAzkabanZip/test.zip
    FileOutputStream fos = new FileOutputStream(jobDir.getPath() + ".zip");
    ZipOutputStream zos = new ZipOutputStream(fos);
    addDirToZipArchive(zos, jobDir, null);
    zos.close();
    fos.close();
    return new File(jobDir.getPath() + ".zip");
  }

  private static void addDirToZipArchive(ZipOutputStream zos, File fileToZip, String parentDirectoryName) throws IOException {
    if (fileToZip == null || !fileToZip.exists()) {
      return;
    }

    String zipEntryName = fileToZip.getName();
    if (parentDirectoryName!=null && !parentDirectoryName.isEmpty()) {
      zipEntryName = parentDirectoryName + "/" + fileToZip.getName();
    }

    if (fileToZip.isDirectory()) {
      for (File file : fileToZip.listFiles()) {
        addDirToZipArchive(zos, file, zipEntryName);
      }
    } else {
      byte[] buffer = new byte[1024];
      FileInputStream fis = new FileInputStream(fileToZip);
      zos.putNextEntry(new ZipEntry(zipEntryName));
      int length;
      while ((length = fis.read(buffer)) > 0) {
        zos.write(buffer, 0, length);
      }
      zos.closeEntry();
      fis.close();
    }
  }
}
