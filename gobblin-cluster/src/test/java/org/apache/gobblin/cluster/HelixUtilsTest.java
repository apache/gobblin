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
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link HelixUtils}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class HelixUtilsTest {

  private Configuration configuration;
  private FileSystem fileSystem;
  private Path tokenFilePath;
  private Token<?> token;

  @BeforeClass
  public void setUp() throws IOException {
    this.configuration = new Configuration();
    this.fileSystem = FileSystem.getLocal(this.configuration);
    this.tokenFilePath = new Path(HelixUtilsTest.class.getSimpleName(), "token");
    this.token = new Token<>();
    this.token.setKind(new Text("test"));
    this.token.setService(new Text("test"));
  }

  @Test
  public void testConfigToProperties() {
    URL url = HelixUtilsTest.class.getClassLoader().getResource(HelixUtilsTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();
    assertEquals(config.getString("k1"), "v1");
    assertEquals(config.getString("k2"), "v1");
    assertEquals(config.getInt("k3"), 1000);
    Assert.assertTrue(config.getBoolean("k4"));
    assertEquals(config.getLong("k5"), 10000);

    Properties properties = ConfigUtils.configToProperties(config);
    assertEquals(properties.getProperty("k1"), "v1");
    assertEquals(properties.getProperty("k2"), "v1");
    assertEquals(properties.getProperty("k3"), "1000");
    assertEquals(properties.getProperty("k4"), "true");
    assertEquals(properties.getProperty("k5"), "10000");
  }

  @Test
  public void testGetWorkunitIdForJobNames() throws GobblinHelixUnexpectedStateException {
    final String HELIX_JOB = "job";
    final String GOBBLIN_JOB_NAME = "gobblin-job-name";

    TaskDriver driver = Mockito.mock(TaskDriver.class);
    WorkflowConfig workflowCfg = Mockito.mock(WorkflowConfig.class);
    JobDag dag = Mockito.mock(JobDag.class);
    JobConfig jobCfg = Mockito.mock(JobConfig.class);
    TaskConfig taskCfg = Mockito.mock(TaskConfig.class);

    /**
     * Mocks for setting up the workflow, job dag, job names, etc.
     *
     * Example of task cfg
     * "mapFields" : {
     *     "006d6d2b-4b8b-4c1b-877b-b7fb51d9295c" : {
     *       "TASK_SUCCESS_OPTIONAL" : "true",
     *       "job.id" : "job_KafkaHdfsStreamingTracking_1668738617409",
     *       "job.name" : "KafkaHdfsStreamingTracking",
     *       "task.id" : "task_KafkaHdfsStreamingTracking_1668738617409_179",
     *       "gobblin.cluster.work.unit.file.path" : "<SOME PATH>",
     *       "TASK_ID" : "006d6d2b-4b8b-4c1b-877b-b7fb51d9295c"
     *     },
     */
    Mockito.when(driver.getWorkflows()).thenReturn(ImmutableMap.of(
        "workflow-1", workflowCfg
    ));

    Mockito.when(workflowCfg.getTargetState()).thenReturn(TargetState.START);
    Mockito.when(workflowCfg.getJobDag()).thenReturn(dag);
    Mockito.when(dag.getAllNodes()).thenReturn(new HashSet<>(Arrays.asList(HELIX_JOB)));
    Mockito.when(driver.getJobConfig(HELIX_JOB)).thenReturn(jobCfg);
    Mockito.when(jobCfg.getTaskConfigMap()).thenReturn(ImmutableMap.of("stub-guid", taskCfg));
    Mockito.when(taskCfg.getConfigMap()).thenReturn(ImmutableMap.of(ConfigurationKeys.JOB_NAME_KEY, GOBBLIN_JOB_NAME));

    assertEquals(
        HelixUtils.getWorkflowIdsFromJobNames(driver, Arrays.asList(GOBBLIN_JOB_NAME)),
        ImmutableMap.of(GOBBLIN_JOB_NAME, "workflow-1"));
  }

  @Test(expectedExceptions = GobblinHelixUnexpectedStateException.class)
  public void testGetWorkunitIdForJobNamesWithInvalidHelixState() throws GobblinHelixUnexpectedStateException {
    final String GOBBLIN_JOB_NAME = "gobblin-job-name";

    TaskDriver driver = Mockito.mock(TaskDriver.class);

    Map<String, WorkflowConfig> workflowConfigMap = new HashMap<>();
    workflowConfigMap.put("null-workflow-to-throw-exception", null);
    Mockito.when(driver.getWorkflows()).thenReturn(workflowConfigMap);

    try {
      HelixUtils.getWorkflowIdsFromJobNames(driver, Arrays.asList(GOBBLIN_JOB_NAME));
    } catch (GobblinHelixUnexpectedStateException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fileSystem.exists(this.tokenFilePath.getParent())) {
      this.fileSystem.delete(this.tokenFilePath.getParent(), true);
    }
  }
}
