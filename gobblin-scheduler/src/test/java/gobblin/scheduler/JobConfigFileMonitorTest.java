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

package gobblin.scheduler;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.configuration.ConfigurationKeys;


/**
 * Unit tests for the job configuration file monitor in {@link gobblin.scheduler.JobScheduler}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.scheduler"})
public class JobConfigFileMonitorTest {

  private static final String JOB_CONFIG_FILE_DIR = "gobblin-test/resource/job-conf";

  private ServiceManager serviceManager;
  private JobScheduler jobScheduler;
  private File newJobConfigFile;

  @BeforeClass
  public void setUp()
      throws Exception {
    Properties properties = new Properties();
    properties.load(new FileReader("gobblin-test/resource/gobblin.test.properties"));
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONFIG_FILE_DIR);
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY, "1000");
    properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    this.jobScheduler = new JobScheduler(properties);
    this.serviceManager = new ServiceManager(Lists.newArrayList(this.jobScheduler));
    this.serviceManager.startAsync();
  }

  @Test
  public void testAddNewJobConfigFile()
      throws Exception {
    Thread.sleep(2000);

    Assert.assertEquals(this.jobScheduler.getScheduledJobs().size(), 3);

    // Create a new job configuration file by making a copy of an existing
    // one and giving a different job name
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(new File(JOB_CONFIG_FILE_DIR, "GobblinTest1.pull")));
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "Gobblin-test-new");
    this.newJobConfigFile = new File(JOB_CONFIG_FILE_DIR, "Gobblin-test-new.pull");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    Thread.sleep(2000);

    Set<String> jobNames = Sets.newHashSet(this.jobScheduler.getScheduledJobs());
    Assert.assertEquals(jobNames.size(), 4);
    Assert.assertTrue(jobNames.contains("GobblinTest1"));
    Assert.assertTrue(jobNames.contains("GobblinTest2"));
    Assert.assertTrue(jobNames.contains("GobblinTest3"));
    // The new job should be in the set of scheduled jobs
    Assert.assertTrue(jobNames.contains("Gobblin-test-new"));
  }

  @Test(dependsOnMethods = {"testAddNewJobConfigFile"})
  public void testChangeJobConfigFile()
      throws Exception {
    Assert.assertEquals(this.jobScheduler.getScheduledJobs().size(), 4);

    // Make a change to the new job configuration file
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(this.newJobConfigFile));
    jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    Thread.sleep(2000);

    Set<String> jobNames = Sets.newHashSet(this.jobScheduler.getScheduledJobs());
    Assert.assertEquals(jobNames.size(), 4);
    Assert.assertTrue(jobNames.contains("GobblinTest1"));
    Assert.assertTrue(jobNames.contains("GobblinTest2"));
    Assert.assertTrue(jobNames.contains("GobblinTest3"));
    // The newly added job should still be in the set of scheduled jobs
    Assert.assertTrue(jobNames.contains("Gobblin-test-new"));
  }

  @Test(dependsOnMethods = {"testChangeJobConfigFile"})
  public void testUnscheduleJob()
      throws Exception {
    Assert.assertEquals(this.jobScheduler.getScheduledJobs().size(), 4);

    // Disable the new job by setting job.disabled=true
    Properties jobProps = new Properties();
    jobProps.load(new FileReader(this.newJobConfigFile));
    jobProps.setProperty(ConfigurationKeys.JOB_DISABLED_KEY, "true");
    jobProps.store(new FileWriter(this.newJobConfigFile), null);

    Thread.sleep(2000);

    Set<String> jobNames = Sets.newHashSet(this.jobScheduler.getScheduledJobs());
    Assert.assertEquals(jobNames.size(), 3);
    Assert.assertTrue(jobNames.contains("GobblinTest1"));
    Assert.assertTrue(jobNames.contains("GobblinTest2"));
    Assert.assertTrue(jobNames.contains("GobblinTest3"));
  }

  @AfterClass
  public void tearDown()
      throws TimeoutException {
    this.newJobConfigFile.delete();
    this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
  }
}
