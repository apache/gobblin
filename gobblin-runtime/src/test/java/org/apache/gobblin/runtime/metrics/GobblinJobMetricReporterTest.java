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
package org.apache.gobblin.runtime.metrics;

import com.codahale.metrics.Metric;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobLauncherTestHelper;
import org.apache.gobblin.runtime.JobState;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



/**
 * Unit test for {@link GobblinJobMetricReporter and its implementation classes}.
 */
@Test(groups = { "gobblin.runtime.local" })
public class GobblinJobMetricReporterTest {

  private Properties launcherProps;
  private JobLauncherTestHelper jobLauncherTestHelper;
  private ITestMetastoreDatabase testMetastoreDatabase;

  @BeforeClass
  public void startUp() throws Exception {
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.launcherProps = new Properties();
    this.launcherProps.load(new FileReader("gobblin-test/resource/gobblin.test.properties"));
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, "true");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "true");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY, "false");
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, testMetastoreDatabase.getJdbcUrl());

    StateStore<JobState.DatasetState> datasetStateStore =
        new FsStateStore<>(this.launcherProps.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
            this.launcherProps.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), JobState.DatasetState.class);

    this.jobLauncherTestHelper = new JobLauncherTestHelper(this.launcherProps, datasetStateStore);
  }

  @Test
  public void testLaunchJobWithDefaultMetricsReporter() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testDefaultMetricsReporter");
    try {
      JobContext jobContext = this.jobLauncherTestHelper.runTest(jobProps);
      Map<String, Metric> metrics = jobContext.getJobMetricsOptional().get().getMetricContext().getMetrics();
      Assert.assertTrue(metrics.containsKey("JobMetrics.WorkUnitsCreationTimer.GobblinTest1-testDefaultMetricsReporter"));

    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }


  @Test
  public void testLaunchJobWithServiceMetricsReporter() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, "true");
    jobProps.setProperty(ConfigurationKeys.JOB_METRICS_REPORTER_CLASS_KEY, ServiceGobblinJobMetricReporter.class.getName());
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "FlowName_FlowGroup_JobName_EdgeId_Hash");
    jobProps.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, "FlowGroup");
    jobProps.setProperty(ConfigurationKeys.FLOW_NAME_KEY, "FlowName");
    jobProps.setProperty("flow.edge.id", "EdgeId");
    try {
      JobContext jobContext = this.jobLauncherTestHelper.runTest(jobProps);
      Map<String, Metric> metrics = jobContext.getJobMetricsOptional().get().getMetricContext().getMetrics();
      Assert.assertTrue(metrics.containsKey("GobblinService.FlowGroup.FlowName.EdgeId.WorkUnitsCreated"));
      Assert.assertTrue(metrics.containsKey("GobblinService.FlowGroup.FlowName.EdgeId.WorkUnitsCreationTimer"));
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    if (testMetastoreDatabase != null) {
      testMetastoreDatabase.close();
    }
  }

  private Properties loadJobProps() throws IOException {
    Properties jobProps = new Properties();
    jobProps.load(new FileReader("gobblin-test/resource/job-conf/GobblinTest1.pull"));
    jobProps.putAll(this.launcherProps);
    jobProps.setProperty(JobLauncherTestHelper.SOURCE_FILE_LIST_KEY,
        "gobblin-test/resource/source/test.avro.0," + "gobblin-test/resource/source/test.avro.1,"
            + "gobblin-test/resource/source/test.avro.2," + "gobblin-test/resource/source/test.avro.3");

    return jobProps;
  }
}