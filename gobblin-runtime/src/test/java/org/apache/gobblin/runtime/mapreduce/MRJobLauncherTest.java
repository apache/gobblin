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

package org.apache.gobblin.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.capability.Capability;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.runtime.JobLauncherTestHelper;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.util.limiter.BaseLimiterType;
import org.apache.gobblin.util.limiter.DefaultLimiterFactory;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;


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

  /**
   * Byteman test that ensures the {@link MRJobLauncher} successfully cleans up all staging data even
   * when an exception is thrown in the {@link MRJobLauncher#countersToMetrics(GobblinMetrics)} method.
   * The {@link BMRule} is to inject an {@link IOException} when the
   * {@link MRJobLauncher#countersToMetrics(GobblinMetrics)} method is called.
   */
  @Test
  @BMRule(name = "testJobCleanupOnError", targetClass = "org.apache.gobblin.runtime.mapreduce.MRJobLauncher",
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

  // This test uses byteman to check that the ".suc" files are recorded in the task state store for successful
  // tasks when there are some task failures.
  // static variable to count the number of task success marker files written in this test case
  public static int sucCount1 = 0;
  @Test
  @BMRules(rules = {
      @BMRule(name = "saveSuccessCount", targetClass = "org.apache.gobblin.metastore.FsStateStore",
          targetMethod = "put", targetLocation = "AT ENTRY", condition = "$2.endsWith(\".suc\")",
          action = "org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.sucCount1 = org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.sucCount1 + 1")
  })
  public void testLaunchJobWithMultiWorkUnitAndFaultyExtractor() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithMultiWorkUnitAndFaultyExtractor");
    jobProps.setProperty("use.multiworkunit", Boolean.toString(true));
    try {
      this.jobLauncherTestHelper.runTestWithCommitSuccessfulTasksPolicy(jobProps);

      // three of the 4 tasks should have succeeded, so 3 suc files should have been written
      Assert.assertEquals(sucCount1, 3);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }
  }

  // This test case checks that if a ".suc" task state file exists for a task then it is skipped.
  // This test also checks that ".suc" files are not written when there are no task failures.
  // static variables accessed by byteman in this test case
  public static WorkUnitState wus = null;
  public static int sucCount2 = 0;
  @Test
  @BMRules(rules = {
      @BMRule(name = "getWorkUnitState", targetClass = "org.apache.gobblin.runtime.GobblinMultiTaskAttempt",
          targetMethod = "runWorkUnits", targetLocation = "AFTER WRITE $taskId", condition = "$taskId.endsWith(\"_1\")",
          action = "org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.wus = new org.apache.gobblin.configuration.WorkUnitState($workUnit, $0.jobState)"),
      @BMRule(name = "saveSuccessCount", targetClass = "org.apache.gobblin.metastore.FsStateStore",
          targetMethod = "put", targetLocation = "AT ENTRY", condition = "$2.endsWith(\".suc\")",
          action = "org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.sucCount2 = org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.sucCount2 + 1"),
      @BMRule(name = "checkProp", targetClass = "org.apache.gobblin.runtime.mapreduce.MRJobLauncher$TaskRunner",
          targetMethod = "setup", targetLocation = "AT EXIT",
          condition = "!$0.jobState.getProp(\"DynamicKey1\").equals(\"DynamicValue1\")",
          action = "throw new RuntimeException(\"could not find key\")"),
      @BMRule(name = "writeSuccessFile", targetClass = "org.apache.gobblin.runtime.GobblinMultiTaskAttempt",
          targetMethod = "taskSuccessfulInPriorAttempt", targetLocation = "AFTER WRITE $taskStateStore",
          condition = "$1.endsWith(\"_1\")",
          action = "$taskStateStore.put($0.jobId, $1 + \".suc\", new org.apache.gobblin.runtime.TaskState(org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest.wus))")
  })
  public void testLaunchJobWithMultiWorkUnitAndSucFile() throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithMultiWorkUnitAndSucFile");
    jobProps.setProperty("use.multiworkunit", Boolean.toString(true));

    jobProps.setProperty("dynamicConfigGenerator.class",
        "org.apache.gobblin.runtime.mapreduce.MRJobLauncherTest$TestDynamicConfigGenerator");

    try {
      this.jobLauncherTestHelper.runTestWithSkippedTask(jobProps, "_1");

      // no failures, so the only success file written is the injected one
      Assert.assertEquals(sucCount2, 1);
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

  /**
   * Seems setting mapreduce.map.failures.maxpercent=100 does not prevent Hadoop2's LocalJobRunner from
   * failing and aborting a job if any mapper task fails. Aborting the job causes its working directory
   * to be deleted in {@link GobblinOutputCommitter}, which further fails this test since all the output
   * {@link org.apache.gobblin.runtime.TaskState}s are deleted. There may be a bug in Hadoop2's LocalJobRunner.
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

  @Test
  public void testLaunchJobWithNonThreadsafeDataPublisher() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testLaunchJobWithNonThreadsafeDataPublisher");
    log.info("in");
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithNonThreadsafeDataPublisher");
    jobProps.setProperty(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE, TestNonThreadsafeDataPublisher.class.getName());

    // make sure the count starts from 0
    TestNonThreadsafeDataPublisher.instantiatedCount.set(0);

    try {
      this.jobLauncherTestHelper.runTestWithMultipleDatasets(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }

    // A different  publisher is used for each dataset
    Assert.assertEquals(TestNonThreadsafeDataPublisher.instantiatedCount.get(), 4);

    log.info("out");
  }

  @Test
  public void testLaunchJobWithThreadsafeDataPublisher() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testLaunchJobWithThreadsafeDataPublisher");
    log.info("in");
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) + "-testLaunchJobWithThreadsafeDataPublisher");
    jobProps.setProperty(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE, TestThreadsafeDataPublisher.class.getName());

    // make sure the count starts from 0
    TestThreadsafeDataPublisher.instantiatedCount.set(0);

    try {
      this.jobLauncherTestHelper.runTestWithMultipleDatasets(jobProps);
    } finally {
      this.jobLauncherTestHelper.deleteStateStore(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    }

    // The same publisher is used for all the data sets
    Assert.assertEquals(TestThreadsafeDataPublisher.instantiatedCount.get(), 1);

    log.info("out");
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

  public static class TestDynamicConfigGenerator implements DynamicConfigGenerator {
    public TestDynamicConfigGenerator() {
    }

    @Override
    public Config generateDynamicConfig(Config config) {
      return ConfigFactory.parseMap(ImmutableMap.of(JobLauncherTestHelper.DYNAMIC_KEY1,
          JobLauncherTestHelper.DYNAMIC_VALUE1));
    }
  }

  public static class TestNonThreadsafeDataPublisher extends DataPublisher {
    // for counting how many times the object is instantiated in the test case
    static AtomicInteger instantiatedCount = new AtomicInteger(0);

    public TestNonThreadsafeDataPublisher(State state) {
      super(state);
      instantiatedCount.incrementAndGet();
    }

    @Override
    public void initialize() throws IOException {
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
      for (WorkUnitState workUnitState : states) {
        // Upon successfully committing the data to the final output directory, set states
        // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
        // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
        workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
    }

    @Override
    public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean supportsCapability(Capability c, Map<String, Object> properties) {
      return c == DataPublisher.REUSABLE;
    }
  }

  public static class TestThreadsafeDataPublisher extends TestNonThreadsafeDataPublisher {
    public TestThreadsafeDataPublisher(State state) {
      super(state);
    }

    @Override
    public boolean supportsCapability(Capability c, Map<String, Object> properties) {
      return (c == Capability.THREADSAFE || c == DataPublisher.REUSABLE);
    }
  }
}
