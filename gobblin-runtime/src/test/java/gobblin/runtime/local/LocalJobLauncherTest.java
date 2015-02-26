/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.local;

import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.runtime.JobLauncherTestBase;
import gobblin.runtime.JobState;
import gobblin.writer.Destination;
import gobblin.writer.WriterOutputFormat;


/**
 * Unit test for {@link LocalJobLauncher}.
 */
@Test(groups = {"gobblin.runtime.local"})
public class LocalJobLauncherTest extends JobLauncherTestBase {

  @BeforeClass
  public void startUp()
      throws Exception {
    this.properties = new Properties();
    this.properties.load(new FileReader("gobblin-test/resource/gobblin.test.properties"));
    this.properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, "true");
    this.properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "true");
    this.properties
        .setProperty(ConfigurationKeys.JOB_HISTORY_STORE_JDBC_DRIVER_KEY, "org.apache.derby.jdbc.EmbeddedDriver");
    this.properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, "jdbc:derby:memory:gobblin1;create=true");
    prepareJobHistoryStoreDatabase(this.properties);
    this.jobStateStore = new FsStateStore(this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
        this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), JobState.class);
  }

  @Test
  public void testLaunchJob()
      throws Exception {
    runTest(loadJobProps());
  }

  @Test
  public void testLaunchJobWithPullLimit()
      throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty(ConfigurationKeys.EXTRACT_PULL_LIMIT, "10");
    runTestWithPullLimit(jobProps);
  }

  @Test
  public void testLaunchJobWithMultiWorkUnit()
      throws Exception {
    Properties jobProps = loadJobProps();
    jobProps.setProperty("use.multiworkunit", Boolean.toString(true));
    runTest(jobProps);
  }

  @Test(groups = {"ignore"})
  public void testCancelJob()
      throws Exception {
    runTestWithCancellation(loadJobProps());
  }

  @Test
  public void testLaunchJobWithFork()
      throws Exception {
    Properties jobProps = loadJobProps();
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
    jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".0", "gobblin-test/basicTest/tmp/taskStaging");
    jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".1", "gobblin-test/basicTest/tmp/taskStaging");
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".0", "gobblin-test/basicTest/tmp/taskOutput");
    jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".1", "gobblin-test/basicTest/tmp/taskOutput");
    jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0", "gobblin-test/jobOutput");
    jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1", "gobblin-test/jobOutput");
    runTestWithFork(jobProps);
  }

  @AfterClass
  public void tearDown() {
    try {
      DriverManager.getConnection("jdbc:derby:memory:gobblin1;shutdown=true");
    } catch (SQLException se) {
      // An exception is expected when shutting down the database
    }
  }

  private Properties loadJobProps()
      throws IOException {
    Properties jobProps = new Properties();
    jobProps.load(new FileReader("gobblin-test/resource/job-conf/GobblinTest1.pull"));
    jobProps.putAll(this.properties);
    jobProps.setProperty(SOURCE_FILE_LIST_KEY, "gobblin-test/resource/source/test.avro.0," +
            "gobblin-test/resource/source/test.avro.1," +
            "gobblin-test/resource/source/test.avro.2," +
            "gobblin-test/resource/source/test.avro.3");

    return jobProps;
  }
}
