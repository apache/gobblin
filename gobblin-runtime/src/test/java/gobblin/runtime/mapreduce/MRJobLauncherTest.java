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

package gobblin.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.metastore.testing.ITestMetastoreDatabase;
import gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.JobLauncherTestHelper;
import gobblin.runtime.JobState;
import gobblin.util.limiter.BaseLimiterType;
import gobblin.util.limiter.DefaultLimiterFactory;
import gobblin.writer.Destination;
import gobblin.writer.WriterOutputFormat;


/**
 * Unit test for {@link MRJobLauncher}.
 */
@Test(groups = { "gobblin.runtime.mapreduce", "gobblin.runtime" }, singleThreaded=true)
public class MRJobLauncherTest extends BMNGRunner {

  private final Logger testLogger = LoggerFactory.getLogger(MRJobLauncherTest.class);
  private Properties launcherProps;
  private JobLauncherTestHelper jobLauncherTestHelper;
  private ITestMetastoreDatabase testMetastoreDatabase;

  @BeforeClass
  public void startUp() throws Exception {
    this.testLogger.info("startUp: in");
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    this.launcherProps = new Properties();

    try (InputStream propsReader = getClass().getClassLoader().getResourceAsStream("gobblin.mr-test.properties")) {
      this.launcherProps.load(propsReader);
    }
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, "true");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "true");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY, "false");
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY,
        testMetastoreDatabase.getJdbcUrl());

    StateStore<JobState.DatasetState> datasetStateStore =
        new FsStateStore<>(this.launcherProps.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
            this.launcherProps.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), JobState.DatasetState.class);

    this.jobLauncherTestHelper = new JobLauncherTestHelper(this.launcherProps, datasetStateStore);

    // Other tests may not clean up properly, clean up outputDir and stagingDir or some of these tests might fail.
    String outputDir = this.launcherProps.getProperty(ConfigurationKeys.WRITER_OUTPUT_DIR);
    String stagingDir = this.launcherProps.getProperty(ConfigurationKeys.WRITER_STAGING_DIR);
    FileUtils.deleteDirectory(new File(outputDir));
    FileUtils.deleteDirectory(new File(stagingDir));
    this.testLogger.info("startUp: out");
  }

  @Test
  public void testLaunchJob() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testLaunchJob");
    log.info("in");
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJob");
    try {
      this.jobLauncherTestHelper.runTest(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
    log.info("out");
  }

  @Test
  public void testLaunchJobWithConcurrencyLimit() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testLaunchJobWithConcurrencyLimit");
    log.info("in");
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithConcurrencyLimit");
    jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "2");
    this.jobLauncherTestHelper.runTest(jobProps);
    jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "3");
    this.jobLauncherTestHelper.runTest(jobProps);
    jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "5");
    try {
      this.jobLauncherTestHelper.runTest(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
    log.info("out");
  }

  @Test
  public void testLaunchJobWithPullLimit() throws Exception {
    int limit = 10;
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithPullLimit");
    jobProps.setProperty(ConfigurationKeys.EXTRACT_LIMIT_ENABLED_KEY, Boolean.TRUE.toString());
    jobProps.setProperty(DefaultLimiterFactory.EXTRACT_LIMIT_TYPE_KEY, BaseLimiterType.COUNT_BASED.toString());
    jobProps.setProperty(DefaultLimiterFactory.EXTRACT_LIMIT_COUNT_LIMIT_KEY, Integer.toString(10));
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

  /**
   * Byteman test that ensures the {@link MRJobLauncher} successfully cleans up all staging data even
   * when an exception is thrown in the {@link MRJobLauncher#countersToMetrics(GobblinMetrics)} method.
   * The {@link BMRule} is to inject an {@link IOException} when the
   * {@link MRJobLauncher#countersToMetrics(GobblinMetrics)} method is called.
   */
  @Test
  @BMRule(name = "testJobCleanupOnError", targetClass = "gobblin.runtime.mapreduce.MRJobLauncher",
      targetMethod = "countersToMetrics(GobblinMetrics)", targetLocation = "AT ENTRY", condition = "true",
      action = "throw new IOException(\"Exception for testJobCleanupOnError\")")
  public void testJobCleanupOnError() throws IOException {
    Properties props = loadJobProps();
    try {
      this.jobLauncherTestHelper.runTest(props);
      Assert.fail("Byteman is not configured properly, the runTest method should have throw an exception");
    } catch (Exception e) {
      // The job should throw an exception, ignore it
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(props.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }

    Assert.assertTrue(props.containsKey(ConfigurationKeys.WRITER_STAGING_DIR));
    Assert.assertTrue(props.containsKey(ConfigurationKeys.WRITER_OUTPUT_DIR));

    File stagingDir = new File(props.getProperty(ConfigurationKeys.WRITER_STAGING_DIR));
    File outputDir = new File(props.getProperty(ConfigurationKeys.WRITER_OUTPUT_DIR));

    Assert.assertEquals(FileUtils.listFiles(stagingDir, null, true).size(), 0);
    if (outputDir.exists()) {
      Assert.assertEquals(FileUtils.listFiles(outputDir, null, true).size(), 0);
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

  /**
   * Seems setting mapreduce.map.failures.maxpercent=100 does not prevent Hadoop2's LocalJobRunner from
   * failing and aborting a job if any mapper task fails. Aborting the job causes its working directory
   * to be deleted in {@link GobblinOutputCommitter}, which further fails this test since all the output
   * {@link gobblin.runtime.TaskState}s are deleted. There may be a bug in Hadoop2's LocalJobRunner.
   *
   * Also applicable to the two tests below.
   */
  @Test(groups = { "ignore" })
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

  @Test(groups = { "ignore" })
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

  @Test(groups = { "ignore" })
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

  public Properties loadJobProps() throws IOException {
    Properties jobProps = new Properties();
    try (InputStream propsReader = getClass().getClassLoader().getResourceAsStream("mr-job-conf/GobblinMRTest.pull")) {
      jobProps.load(propsReader);
    }
    jobProps.putAll(this.launcherProps);
    jobProps.setProperty(JobLauncherTestHelper.SOURCE_FILE_LIST_KEY,
        "gobblin-test/resource/source/test.avro.0," + "gobblin-test/resource/source/test.avro.1,"
            + "gobblin-test/resource/source/test.avro.2," + "gobblin-test/resource/source/test.avro.3");

    return jobProps;
  }
}
