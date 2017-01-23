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

package gobblin.service;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.client.RestLiResponseException;
import com.typesafe.config.Config;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.job_catalog.FSJobCatalog;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;

import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.service" })
public class JobConfigServerTest {
  private JobConfigClient client;
  private JobConfigServer server;
  private File testDirectory;

  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_JOB_NAME = "testJob1";
  private static final String TEST_SCHEDULE = "";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME = "dummyGroup";
  private static final String TEST_DUMMY_JOB_NAME = "dummyJob";

  @BeforeClass
  public void setUp() throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();
    Properties properties = new Properties();

    testDirectory = Files.createTempDir();

    int randomPort = chooseRandomPort();
    configBuilder.addPrimitive(JobConfigServer.SERVICE_PORT_KEY, Integer.toString(randomPort));
    configBuilder.addPrimitive(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, testDirectory.getAbsolutePath());

    Config config = configBuilder.build();
    MutableJobCatalog jobCatalog = new FSJobCatalog(config);

    client =
        new JobConfigClient(String.format("http://%s:%s/", JobConfigServer.DEFAULT_SERVICE_HOST, randomPort));
    server = new JobConfigServer(config, jobCatalog);
    server.startUp();
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("param1", "value1");

    JobConfig jobConfig = new JobConfig().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME)
        .setTemplateUri(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(jobProperties));

    client.createJobConfig(jobConfig);
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("param1", "value1");

    JobConfig jobConfig = new JobConfig().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME)
        .setTemplateUri(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(jobProperties));

    try {
      client.createJobConfig(jobConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.CONFLICT_409);
      return;
    }

    Assert.fail("Get should have gotten a 409 error");
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
    JobConfigId jobConfigId = new JobConfigId().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME);
    JobConfig jobConfig = client.getJobConfig(jobConfigId);

    Assert.assertEquals(jobConfig.getJobGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(jobConfig.getJobName(), TEST_JOB_NAME);
    Assert.assertEquals(jobConfig.getSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(jobConfig.getTemplateUri(), TEST_TEMPLATE_URI);
    // Add this asssert back when getJobSpec() is changed to return the raw job spec
    //Assert.assertEquals(jobConfig.getProperties().size(), 1);
    Assert.assertEquals(jobConfig.getProperties().get("param1"), "value1");
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    JobConfigId jobConfigId = new JobConfigId().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME);

    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("param1", "value1b");
    jobProperties.put("param2", "value2b");

    JobConfig jobConfig = new JobConfig().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME)
        .setTemplateUri(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(jobProperties));

    client.updateJobConfig(jobConfig);

    JobConfig retrievedJobConfig = client.getJobConfig(jobConfigId);

    Assert.assertEquals(retrievedJobConfig.getJobGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(retrievedJobConfig.getJobName(), TEST_JOB_NAME);
    Assert.assertEquals(retrievedJobConfig.getSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(retrievedJobConfig.getTemplateUri(), TEST_TEMPLATE_URI);
    // Add this asssert when getJobSpec() is changed to return the raw job spec
    //Assert.assertEquals(jobConfig.getProperties().size(), 2);
    Assert.assertEquals(retrievedJobConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedJobConfig.getProperties().get("param2"), "value2b");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testDelete() throws Exception {
    JobConfigId jobConfigId = new JobConfigId().setJobGroup(TEST_GROUP_NAME).setJobName(TEST_JOB_NAME);

    // make sure job config exists
    JobConfig jobConfig = client.getJobConfig(jobConfigId);
    Assert.assertEquals(jobConfig.getJobGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(jobConfig.getJobName(), TEST_JOB_NAME);

    client.deleteJobConfig(jobConfigId);

    try {
      client.getJobConfig(jobConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have gotten a 404 error");
  }

  @Test
  public void testBadGet() throws Exception {
    JobConfigId jobConfigId = new JobConfigId().setJobGroup(TEST_DUMMY_GROUP_NAME).setJobName(TEST_DUMMY_JOB_NAME);

    try {
      client.getJobConfig(jobConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadDelete() throws Exception {
    JobConfigId jobConfigId = new JobConfigId().setJobGroup(TEST_DUMMY_GROUP_NAME).setJobName(TEST_DUMMY_JOB_NAME);

    try {
      client.getJobConfig(jobConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadUpdate() throws Exception {
    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("param1", "value1b");
    jobProperties.put("param2", "value2b");

    JobConfig jobConfig = new JobConfig().setJobGroup(TEST_DUMMY_GROUP_NAME).setJobName(TEST_DUMMY_JOB_NAME)
        .setTemplateUri(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(jobProperties));

    try {
      client.updateJobConfig(jobConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (this.client != null) {
      this.client.close();
    }
    if (this.server != null) {
      this.server.shutDown();
    }
    testDirectory.delete();
  }

  private static int chooseRandomPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }
}
