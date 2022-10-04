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

package org.apache.gobblin.runtime;

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
import org.apache.gobblin.runtime.local.LocalJobLauncher;
import org.apache.gobblin.runtime.metrics.ServiceGobblinJobMetricReporter;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.limiter.BaseLimiterType;
import org.apache.gobblin.util.limiter.DefaultLimiterFactory;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import static org.apache.gobblin.runtime.AbstractJobLauncher.GOBBLIN_JOB_MULTI_TEMPLATE_KEY;
import static org.apache.gobblin.runtime.AbstractJobLauncher.GOBBLIN_JOB_TEMPLATE_KEY;


/**
 * Unit test for {@link LocalJobLauncher}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.runtime.local" })
public class LocalJobLauncherTest {

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
  public void testLaunchJob() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJob");
    try {
      this.jobLauncherTestHelper.runTest(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testJobTemplateResolutionInAbstractLauncher() throws Exception {
    Properties jobProps = loadJobProps();
    String jobId = JobLauncherUtils.newJobId("beforeResolution");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty("job.name", "beforeResolution");
    jobProps.setProperty(GOBBLIN_JOB_TEMPLATE_KEY, "resource:///templates/distcp-ng.template");

    JobContext jobContext = dummyJobContextInitHelper(jobProps);

    // Indicating resolution succeeded.
    // 1) User config not being overloaded by template
    // 2) Conf that not appearing in the user-config being populated by template
    System.out.println(jobContext.getJobState());
    Assert.assertEquals(jobContext.getJobState().getProp("job.name"), "beforeResolution");
    Assert.assertEquals(jobContext.getJobState().getProp("distcp.persist.dir"), "/tmp/distcp-persist-dir");
  }

  @Test
  public void testMultipleJobTemplatesResoluion() throws Exception {
    Properties jobProps = loadJobProps();
    // Job Name shouldn't be overwritten by any templates and the precedence of template is lower than job configuration.
    String jobId = JobLauncherUtils.newJobId("beforeResolution");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty("job.name", "beforeResolution");
    jobProps.setProperty(GOBBLIN_JOB_MULTI_TEMPLATE_KEY,
        "resource:///templates/test.template,resource:///templates/test-overwrite.template");

    JobContext jobContext = dummyJobContextInitHelper(jobProps);

    // Verifying multi-resolution happens and it respect the precedency.
    Assert.assertEquals(jobContext.getJobState().getProp("job.name"), "beforeResolution");
    Assert.assertEquals(jobContext.getJobState().getProp("templated0"), "x_x");
    Assert.assertEquals(jobContext.getJobState().getProp("templated1"), "y_y");

    // Verifying the order of template list matters.
    jobProps.setProperty(GOBBLIN_JOB_MULTI_TEMPLATE_KEY,
        "resource:///templates/test-overwrite.template,resource:///templates/test.template");
    jobContext = dummyJobContextInitHelper(jobProps);
    Assert.assertEquals(jobContext.getJobState().getProp("job.name"), "beforeResolution");
    Assert.assertEquals(jobContext.getJobState().getProp("templated0"), "x");
    Assert.assertEquals(jobContext.getJobState().getProp("templated1"), "y");

    // Verify multi-resolution with inheritance.
    jobProps.setProperty(GOBBLIN_JOB_MULTI_TEMPLATE_KEY,
        "resource:///templates/test-multitemplate-with-inheritance.template,resource:///templates/test.template");
    jobContext = dummyJobContextInitHelper(jobProps);
    Assert.assertEquals(jobContext.getJobState().getProp("templated0"), "x");
    Assert.assertEquals(jobContext.getJobState().getProp("templated1"), "y");
    Assert.assertEquals(jobContext.getJobState().getProp("job.name"), "beforeResolution");
    // Picked an distcp-specific configuration that there's no default value being set in jobConf.
    Assert.assertEquals(jobContext.getJobState().getProp("distcp.persist.dir"), "/tmp/distcp-persist-dir");
  }

  /**
   * Initialize a jobContext by initializing jobLauncher. This code is mostly used for
   * testing job templates resolution.
   */
  private JobContext dummyJobContextInitHelper(Properties jobProps) throws Exception {
    JobContext jobContext = null;
    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      return ((AbstractJobLauncher) jobLauncher).getJobContext();
    } finally {
      closer.close();
    }
  }

  @Test
  public void testLaunchJobWithPullLimit() throws Exception {
    int limit = 10;
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithPullLimit");
    jobProps.setProperty(ConfigurationKeys.EXTRACT_LIMIT_ENABLED_KEY, Boolean.TRUE.toString());
    jobProps.setProperty(DefaultLimiterFactory.EXTRACT_LIMIT_TYPE_KEY, BaseLimiterType.COUNT_BASED.toString());
    jobProps.setProperty(DefaultLimiterFactory.EXTRACT_LIMIT_COUNT_LIMIT_KEY, Integer.toString(limit));
    try {
      this.jobLauncherTestHelper.runTestWithPullLimit(jobProps, limit);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithMultiWorkUnit() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithMultiWorkUnit");
    jobProps.setProperty("use.multiworkunit", Boolean.toString(true));
    try {
      this.jobLauncherTestHelper.runTest(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithTaskTimestamps() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJob");
    jobProps.setProperty(ConfigurationKeys.WRITER_ADD_TASK_TIMESTAMP, "true");

    try {
      this.jobLauncherTestHelper.runTest(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test(groups = {"ignore"})
  public void testCancelJob() throws Exception {
    this.jobLauncherTestHelper.runTestWithCancellation(loadJobProps());
  }

  @Test
  public void testLaunchJobWithFork() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithFork");
    jobProps.setProperty(ConfigurationKeys.CONVERTER_CLASSES_KEY, "org.apache.gobblin.test.TestConverter2");
    jobProps.setProperty(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".0",
        "org.apache.gobblin.policies.schema.SchemaRowCheckPolicy");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".1",
        "org.apache.gobblin.policies.schema.SchemaRowCheckPolicy");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".0", "OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".1", "OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".0",
        "org.apache.gobblin.policies.count.RowCountPolicy,org.apache.gobblin.policies.schema.SchemaCompatibilityPolicy");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".1",
        "org.apache.gobblin.policies.count.RowCountPolicy,org.apache.gobblin.policies.schema.SchemaCompatibilityPolicy");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE + ".0", "OPTIONAL,OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE + ".1", "OPTIONAL,OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".0", WriterOutputFormat.AVRO.name());
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".1", WriterOutputFormat.AVRO.name());
    jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".0", Destination.DestinationType.HDFS.name());
    jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".1", Destination.DestinationType.HDFS.name());
    jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".0",
        jobProps.getProperty(ConfigurationKeys.WRITER_STAGING_DIR));
    jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".1",
        jobProps.getProperty(ConfigurationKeys.WRITER_STAGING_DIR));
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".0",
        jobProps.getProperty(ConfigurationKeys.WRITER_OUTPUT_DIR));
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".1",
        jobProps.getProperty(ConfigurationKeys.WRITER_OUTPUT_DIR));
    jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0",
        jobProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
    jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1",
        jobProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
    try {
      this.jobLauncherTestHelper.runTestWithFork(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithMultipleDatasets() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithMultipleDatasets");
    try {
      this.jobLauncherTestHelper.runTestWithMultipleDatasets(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithCommitSuccessfulTasksPolicy() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithCommitSuccessfulTasksPolicy");
    try {
      this.jobLauncherTestHelper.runTestWithCommitSuccessfulTasksPolicy(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithMultipleDatasetsAndFaultyExtractor() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithMultipleDatasetsAndFaultyExtractor");
    try {
      this.jobLauncherTestHelper.runTestWithMultipleDatasetsAndFaultyExtractor(jobProps, false);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  @Test
  public void testLaunchJobWithMultipleDatasetsAndFaultyExtractorAndPartialCommitPolicy() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)
        + "-testLaunchJobWithMultipleDatasetsAndFaultyExtractorAndPartialCommitPolicy");
    try {
      this.jobLauncherTestHelper.runTestWithMultipleDatasetsAndFaultyExtractor(jobProps, true);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
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