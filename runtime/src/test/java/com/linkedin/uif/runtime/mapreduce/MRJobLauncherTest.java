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

package com.linkedin.uif.runtime.mapreduce;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.runtime.JobLauncherTestBase;
import com.linkedin.uif.runtime.JobState;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;

/**
 * Unit test for {@link MRJobLauncher}.
 */
@Test(groups = {"ignore", "com.linkedin.uif.runtime.mapreduce"})
public class MRJobLauncherTest extends JobLauncherTestBase {

    @BeforeClass
    public void startUp() throws Exception {
        this.properties = new Properties();
        this.properties.load(new FileReader("test/resource/uif.mr-test.properties"));
        this.properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "true");
        this.jobStateStore = new FsStateStore(
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
    }

    @Test
    public void testLaunchJob() throws Exception {
        runTest(loadJobProps());
    }

    @Test
    public void testLaunchJobWithConcurrencyLimit() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "2");
        runTest(jobProps);
        jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "3");
        runTest(jobProps);
        jobProps.setProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, "5");
        runTest(jobProps);
    }

    @Test
    public void testLaunchJobWithPullLimit() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty(ConfigurationKeys.EXTRACT_PULL_LIMIT, "10");
        runTestWithPullLimit(jobProps);
    }

    @Test
    public void testLaunchJobWithMultiWorkUnit() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty("use.multiworkunit", Boolean.toString(true));
        runTest(jobProps);
    }

    @Test
    public void testCancelJob() throws Exception {
        runTestWithCancellation(loadJobProps());
    }

    @Test
    public void testLaunchJobWithFork() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty(ConfigurationKeys.CONVERTER_CLASSES_KEY, "com.linkedin.uif.test.TestConverter2");
        jobProps.setProperty(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
        jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".0",
                "com.linkedin.uif.policies.schema.SchemaRowCheckPolicy");
        jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + ".1",
                "com.linkedin.uif.policies.schema.SchemaRowCheckPolicy");
        jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".0", "OPTIONAL");
        jobProps.setProperty(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE + ".1", "OPTIONAL");
        jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".0",
                "com.linkedin.uif.policies.count.RowCountPolicy," +
                "com.linkedin.uif.policies.schema.SchemaCompatibilityPolicy");
        jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST + ".1",
                "com.linkedin.uif.policies.count.RowCountPolicy," +
                "com.linkedin.uif.policies.schema.SchemaCompatibilityPolicy");
        jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE + ".0", "OPTIONAL,OPTIONAL");
        jobProps.setProperty(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE + ".1", "OPTIONAL,OPTIONAL");
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".0",
                WriterOutputFormat.AVRO.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".1",
                WriterOutputFormat.AVRO.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".0",
                Destination.DestinationType.HDFS.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".1",
                Destination.DestinationType.HDFS.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".0",
                "test/basicTest/tmp/taskStaging");
        jobProps.setProperty(ConfigurationKeys.WRITER_STAGING_DIR + ".1",
                "test/basicTest/tmp/taskStaging");
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".0",
                "test/basicTest/tmp/taskOutput");
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR + ".1",
                "test/basicTest/tmp/taskOutput");
        jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0",
                "test/jobOutput");
        jobProps.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1",
                "test/jobOutput");
        runTestWithFork(jobProps);
    }

    private Properties loadJobProps() throws IOException {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/mr-job-conf/GobblinMRTest.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.0," +
                "test/resource/source/test.avro.1," +
                "test/resource/source/test.avro.2," +
                "test/resource/source/test.avro.3");

        return jobProps;
    }
}
