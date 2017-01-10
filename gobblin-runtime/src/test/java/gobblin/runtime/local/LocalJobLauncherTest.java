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

package gobblin.runtime.local;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;

import gobblin.metastore.testing.ITestMetastoreDatabase;
import gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import gobblin.runtime.JobLauncherTestHelper;
import gobblin.runtime.JobState;
import gobblin.util.limiter.BaseLimiterType;
import gobblin.util.limiter.DefaultLimiterFactory;
import gobblin.writer.Destination;
import gobblin.writer.WriterOutputFormat;


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
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY,
        testMetastoreDatabase.getJdbcUrl());

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

  @Test(groups = { "ignore" })
  public void testCancelJob() throws Exception {
    this.jobLauncherTestHelper.runTestWithCancellation(loadJobProps());
  }

  @Test
  public void testLaunchJobWithFork() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithFork");
    jobProps.setProperty(ConfigurationKeys.CONVERTER_CLASSES_KEY, "gobblin.test.TestConverter2");
    jobProps.setProperty(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".0",
        "gobblin.policies.schema.SchemaRowCheckPolicy");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".1",
        "gobblin.policies.schema.SchemaRowCheckPolicy");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".0", "OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".1", "OPTIONAL");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".0",
        "gobblin.policies.count.RowCountPolicy,gobblin.policies.schema.SchemaCompatibilityPolicy");
    jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".1",
        "gobblin.policies.count.RowCountPolicy,gobblin.policies.schema.SchemaCompatibilityPolicy");
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
