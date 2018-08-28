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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * This test is disabled by default because it assumes the Azkaban-solo-server is setup on localhost:8081.
 *
 * Please check https://azkaban.github.io/azkaban/docs/latest/ for how to setup Azkaban-solo-server.
 */
@Slf4j
public class AzkabanClientTest {
  private AzkabanClient client = null;
  private FileSystem fs = null;
  private long sessionExpireInMin = 1;
  @BeforeClass
  public void setup() throws Exception {
    Config azkConfig = ConfigFactory.load("local-azkaban-service.conf");
    String userName = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_USERNAME_KEY);
    String password = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_PASSWORD_KEY);
    String url = azkConfig.getString(ServiceAzkabanConfigKeys.AZKABAN_SERVER_URL_KEY);
    this.client = AzkabanClientBuilder.create()
        .setUserName(userName)
        .setPassword(password)
        .setUrl(url)
        .setSessionExpireInMin(sessionExpireInMin)
        .build();
    String uri = ConfigurationKeys.LOCAL_FS_URI;
    this.fs = FileSystem.get(URI.create(uri), new Configuration());
  }

  @AfterClass
  public void cleanup() throws IOException {
    this.client.close();
  }

  private void ensureProjectExist(String projectName, String description) {
    AzkabanClientStatus status;
    // make sure it is in a clean state
    status = this.client.deleteProject(projectName);
    Assert.assertTrue(status.isSuccess());

    // make sure the project is created successfully
    status = this.client.createProject(projectName, description);
    Assert.assertTrue(status.isSuccess());
  }

  @Test(enabled = false)
  public void testCreateProject() {
    String projectName = "project-create";
    String description = "This is a create project test.";
    AzkabanClientStatus status;

    ensureProjectExist(projectName, description);

    // the second time creation should fail
    status = this.client.createProject(projectName, description);
    Assert.assertFalse(status.isSuccess());
  }

  @Test(enabled = false)
  public void testDeleteProject() {
    String projectName = "project-delete";
    String description = "This is a delete project test.";
    AzkabanClientStatus status;

    ensureProjectExist(projectName, description);

    // delete the new project
    status = this.client.deleteProject(projectName);
    Assert.assertTrue(status.isSuccess());
  }

  @Test(enabled = false)
  public void testUploadZip() throws IOException {
    String projectName = "project-upload";
    String description = "This is a upload project test.";
    String flowName = "test-upload";
    AzkabanClientStatus status;

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    status = this.client.uploadProjectZip(projectName, zipFile);
    Assert.assertTrue(status.isSuccess());

    // upload Zip to an non-existed project
    status = this.client.uploadProjectZip("Non-existed-project", zipFile);
    Assert.assertFalse(status.isSuccess());
  }

  @Test(enabled = false)
  public void testExecuteFlow() throws IOException {
    String projectName = "project-execFlow";
    String description = "This is a flow execution test.";
    String flowName = "test-exec-flow";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    AzkabanClientStatus status = this.client.uploadProjectZip(projectName, zipFile);
    Assert.assertTrue(status.isSuccess());

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlow(projectName, flowName, Maps.newHashMap());
    Assert.assertTrue(execStatus.isSuccess());
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  @Test(enabled = false)
  public void testExecuteFlowWithParams() throws IOException {
    String projectName = "project-execFlow-Param";
    String description = "This is a flow execution test.";
    String flowName = "test-exec-flow-param";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    AzkabanClientStatus status = this.client.uploadProjectZip(projectName, zipFile);
    Assert.assertTrue(status.isSuccess());

    Map<String, String> flowParams = Maps.newHashMap();
    flowParams.put("gobblin.source", "DummySource");
    flowParams.put("gobblin.dataset.pattern", "/data/tracking/MessageActionEvent/hourly/*/*/*/*");

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlow(projectName, flowName, flowParams);
    Assert.assertTrue(execStatus.isSuccess());
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  @Test(enabled = false)
  public void testExecuteFlowWithOptions() throws IOException {
    String projectName = "project-execFlow-Option";
    String description = "This is a flow execution test.";
    String flowName = "test-exec-flow-options";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    AzkabanClientStatus status = this.client.uploadProjectZip(projectName, zipFile);
    Assert.assertTrue(status.isSuccess());

    Map<String, String> flowOptions = Maps.newHashMap();

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlowWithOptions(projectName, flowName, flowOptions, Maps.newHashMap());
    Assert.assertTrue(execStatus.isSuccess());
    log.info("Execid: {}", execStatus.getResponse().execId);
  }

  @Test(enabled = false)
  public void testFetchFlowExecution() throws Exception {
    String projectName = "project-fetch-flow-exec";
    String description = "This is a flow execution fetch test.";
    String flowName = "test-fetch-flow-executions";

    ensureProjectExist(projectName, description);

    // upload Zip to project
    File zipFile = createAzkabanZip(flowName);
    AzkabanClientStatus status = this.client.uploadProjectZip(projectName, zipFile);
    Assert.assertTrue(status.isSuccess());

    Map<String, String> flowOptions = Maps.newHashMap();

    // execute a flow
    AzkabanExecuteFlowStatus execStatus = this.client.executeFlowWithOptions(projectName, flowName, flowOptions, Maps.newHashMap());
    Assert.assertTrue(execStatus.isSuccess());
    log.info("Execid: {}", execStatus.getResponse().execId);

    // wait for the job started and failed
    Thread.sleep(3000);

    // job should fail
    AzkabanFetchExecuteFlowStatus fetchExecuteFlowStatus = this.client.fetchFlowExecution(execStatus.getResponse().execId);
    Assert.assertTrue(fetchExecuteFlowStatus.isSuccess());
  }

  @Test(enabled = false)
  public void testSessionExpiration() throws Exception {
    String projectName = "project-session-expiration-test";
    String description = "This is a session expiration test.";
    Thread.sleep(sessionExpireInMin * 60 * 1000);
    ensureProjectExist(projectName, description);
  }

  private File createAzkabanZip(String flowName) throws IOException {
    Properties jobProps = new Properties();
    jobProps.load(this.getClass().getClassLoader().
        getResourceAsStream("azkakaban-job-basic.properties"));

    String basePath = "/tmp/testAzkabanZip";
    this.fs.delete(new Path(basePath), true);

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
