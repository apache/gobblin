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

package gobblin.scheduler;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.configuration.ConfigurationKeys;
import gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for the job configuration file monitor in {@link gobblin.scheduler.JobScheduler}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.scheduler"})
public class JobConfigFileMonitorTest {

  private static final String JOB_CONFIG_FILE_DIR = "gobblin-test/resource/job-conf";

  private String jobConfigDir;
  private ServiceManager serviceManager;
  private JobScheduler jobScheduler;
  private File newJobConfigFile;

  private class GetNumScheduledJobs implements Function<Void, Integer> {

    @Override
    public Integer apply(Void input) {
      return JobConfigFileMonitorTest.this.jobScheduler.getScheduledJobs().size();
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    this.jobConfigDir =
        Files.createTempDirectory(String.format("gobblin-test_%s_job-conf", this.getClass().getSimpleName()))
            .toString();

    FileUtils.forceDeleteOnExit(new File(this.jobConfigDir));
    FileUtils.copyDirectory(new File(JOB_CONFIG_FILE_DIR), new File(jobConfigDir));

    Properties properties = new Properties();
    try (Reader schedulerPropsReader = new FileReader("gobblin-test/resource/gobblin.test.properties")) {
      properties.load(schedulerPropsReader);
    }
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, jobConfigDir);
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, jobConfigDir);
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY, "1000");
    properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    SchedulerService quartzService = new SchedulerService(new Properties());
    this.jobScheduler = new JobScheduler(properties, quartzService);
    this.serviceManager = new ServiceManager(Lists.newArrayList(quartzService, this.jobScheduler));
    this.serviceManager.startAsync().awaitHealthy(10, TimeUnit.SECONDS);;
  }

  @Test
  public void testAddNewJobConfigFile() throws Exception {
    final Logger log = LoggerFactory.getLogger("testAddNewJobConfigFile");
    log.info("testAddNewJobConfigFile: start");
    AssertWithBackoff assertWithBackoff = AssertWithBackoff.create().logger(log).timeoutMs(15000);
    assertWithBackoff.assertEquals(new GetNumScheduledJobs(), 3, "3 scheduled jobs");

    /* Set a time gap, to let the monitor recognize the "3-file" status as old status,
    so that new added file can be discovered */
    Thread.sleep(1000);

    // Create a new job configuration file by making a copy of an existing
    // one and giving a different job name
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(new File(this.jobConfigDir, "GobblinTest1.pull")));
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "Gobblin-test-new");
    this.newJobConfigFile = new File(this.jobConfigDir, "Gobblin-test-new.pull");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    assertWithBackoff.assertEquals(new GetNumScheduledJobs(), 4, "4 scheduled jobs");

    Set<String> jobNames = Sets.newHashSet(this.jobScheduler.getScheduledJobs());
    Set<String> expectedJobNames =
        ImmutableSet.<String>builder()
          .add("GobblinTest1", "GobblinTest2", "GobblinTest3", "Gobblin-test-new")
          .build();

    Assert.assertEquals(jobNames, expectedJobNames);
    log.info("testAddNewJobConfigFile: end");
  }

  @Test(dependsOnMethods = {"testAddNewJobConfigFile"})
  public void testChangeJobConfigFile()
      throws Exception {
    final Logger log = LoggerFactory.getLogger("testChangeJobConfigFile");
    log.info("testChangeJobConfigFile: start");
    Assert.assertEquals(this.jobScheduler.getScheduledJobs().size(), 4);

    // Make a change to the new job configuration file
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(this.newJobConfigFile));
    jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "Gobblin-test-new2");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    AssertWithBackoff.create()
        .logger(log)
        .timeoutMs(30000)
        .assertEquals(new GetNumScheduledJobs(), 4, "4 scheduled jobs");

    final Set<String> expectedJobNames =
        ImmutableSet.<String>builder()
          .add("GobblinTest1", "GobblinTest2", "GobblinTest3", "Gobblin-test-new2")
          .build();
    AssertWithBackoff.create()
      .logger(log)
      .timeoutMs(30000)
      .assertEquals(new Function<Void, Set<String>>() {
          @Override public Set<String> apply(Void input) {
            return Sets.newHashSet(JobConfigFileMonitorTest.this.jobScheduler.getScheduledJobs());
          }
        }, expectedJobNames, "Job change detected");
    log.info("testChangeJobConfigFile: end");
  }

  @Test(dependsOnMethods = {"testChangeJobConfigFile"})
  public void testUnscheduleJob()
      throws Exception {
    final Logger log = LoggerFactory.getLogger("testUnscheduleJob");
    log.info("testUnscheduleJob: start");
    Assert.assertEquals(this.jobScheduler.getScheduledJobs().size(), 4);

    // Disable the new job by setting job.disabled=true
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(this.newJobConfigFile));
    jobProps.setProperty(ConfigurationKeys.JOB_DISABLED_KEY, "true");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    AssertWithBackoff.create()
        .logger(log)
        .timeoutMs(7500)
        .assertEquals(new GetNumScheduledJobs(), 3, "3 scheduled jobs");

    Set<String> jobNames = Sets.newHashSet(this.jobScheduler.getScheduledJobs());
    Assert.assertEquals(jobNames.size(), 3);
    Assert.assertTrue(jobNames.contains("GobblinTest1"));
    Assert.assertTrue(jobNames.contains("GobblinTest2"));
    Assert.assertTrue(jobNames.contains("GobblinTest3"));
    log.info("testUnscheduleJob: end");
  }

  @AfterClass
  public void tearDown()
      throws TimeoutException, IOException {
    if (jobConfigDir != null) {
      FileUtils.forceDelete(new File(jobConfigDir));
    }
    this.serviceManager.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
  }
}
