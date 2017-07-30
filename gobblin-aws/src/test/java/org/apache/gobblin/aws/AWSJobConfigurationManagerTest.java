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

package gobblin.aws;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.event.NewJobConfigArrivalEvent;


/**
 * Unit tests for {@link AWSJobConfigurationManager}.
 *
 * @author Abhishek Tiwari
 */
@Test(groups = { "gobblin.aws" })
public class AWSJobConfigurationManagerTest {
  private static final int NUM_JOB_CONFIG_FILES = 1;

  private static final String JOB_NAME_KEY = "job.name";
  private static final String JOB_FIRST_NAME = "PullFromWikipedia1";
  private static final String JOB_FIRST_ZIP = "wikipedia1.zip";
  private static final String JOB_SECOND_NAME = "PullFromWikipedia2";
  private static final String JOB_SECOND_ZIP = "wikipedia2.zip";
  private static final String URI_ZIP_NAME = "wikipedia.zip";

  private static final String JOB_CONFIG_DIR_NAME = AWSJobConfigurationManagerTest.class.getSimpleName();

  private final File jobConfigFileDir = new File(JOB_CONFIG_DIR_NAME + "_" + System.currentTimeMillis());
  private final EventBus eventBus = new EventBus();
  private AWSJobConfigurationManager jobConfigurationManager;

  private final List<Properties> receivedJobConfigs = Lists.newLinkedList();
  private final CountDownLatch countDownLatchBootUp = new CountDownLatch(NUM_JOB_CONFIG_FILES);
  private final CountDownLatch countDownLatchUpdate = new CountDownLatch(NUM_JOB_CONFIG_FILES);

  @BeforeClass
  public void setUp() throws Exception {
    this.eventBus.register(this);

    // Prepare the test url to download the job conf from
    final URL url = GobblinAWSClusterLauncherTest.class.getClassLoader().getResource(JOB_FIRST_ZIP);
    final String jobConfZipUri = getJobConfigZipUri(new File(url.toURI()));

    // Prepare the test dir to download the job conf to
    if (this.jobConfigFileDir.exists()) {
      FileUtils.deleteDirectory(this.jobConfigFileDir);
    }
    Assert.assertTrue(this.jobConfigFileDir.mkdirs(), "Failed to create " + this.jobConfigFileDir);

    final Config config = ConfigFactory.empty()
        .withValue(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY, ConfigValueFactory.fromAnyRef(this.jobConfigFileDir.toString()))
        .withValue(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY, ConfigValueFactory.fromAnyRef(jobConfZipUri))
        .withValue(GobblinAWSConfigurationKeys.JOB_CONF_REFRESH_INTERVAL, ConfigValueFactory.fromAnyRef("10s"));
    this.jobConfigurationManager = new AWSJobConfigurationManager(this.eventBus, config);
    this.jobConfigurationManager.startAsync().awaitRunning();
  }

  @Test(enabled = false)
  private String getJobConfigZipUri(File source) throws IOException {
    final File destination = new File(StringUtils.substringBeforeLast(source.toString(), File.separator) + File.separator
        + URI_ZIP_NAME);
    if (destination.exists()) {
      if (!destination.delete()) {
        throw new IOException("Cannot clean destination job conf zip file: " + destination);
      }
    }
    FileUtils.copyFile(source, destination);

    return destination.toURI().toString();
  }

  @Test
  public void testBootUpNewJobConfigs() throws Exception {
    // Wait for all job configs to be received
    this.countDownLatchBootUp.await();

    // Wikipedia1.zip has only 1 conf file, so we should only receive that
    Assert.assertEquals(this.receivedJobConfigs.size(), 1);
    Assert.assertEquals(this.receivedJobConfigs.get(0).getProperty(JOB_NAME_KEY), JOB_FIRST_NAME);

  }

  @Test(dependsOnMethods = "testBootUpNewJobConfigs")
  public void testUpdatedNewJobConfigs() throws Exception {
    // Change zip file in the Uri that JobConfigManager is watching
    final URL url = GobblinAWSClusterLauncherTest.class.getClassLoader().getResource(JOB_SECOND_ZIP);
    final String jobConfZipUri = getJobConfigZipUri(new File(url.toURI()));

    // Wait for all job configs to be received (after scheduled execution of 1 minute)
    this.countDownLatchUpdate.await();

    // Wikipedia2.zip has only 2 conf files:
    // 1. The original job conf that is not changed
    // 2. A new job conf that has been added
    // So, we should only receive one new / updated job conf (ie. total number of configs = 2)
    Assert.assertEquals(this.receivedJobConfigs.size(), 2);
    Assert.assertEquals(this.receivedJobConfigs.get(1).getProperty(JOB_NAME_KEY), JOB_SECOND_NAME);
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.jobConfigurationManager.stopAsync().awaitTerminated();
    if (this.jobConfigFileDir.exists()) {
      FileUtils.deleteDirectory(this.jobConfigFileDir);
    }
  }

  @Test(enabled = false)
  @Subscribe
  public void handleNewJobConfigArrival(NewJobConfigArrivalEvent newJobConfigArrivalEvent) {
    Properties jobConfig = newJobConfigArrivalEvent.getJobConfig();
    this.receivedJobConfigs.add(jobConfig);

    if (jobConfig.getProperty(JOB_NAME_KEY).equalsIgnoreCase(JOB_FIRST_NAME)) {
      this.countDownLatchBootUp.countDown();
    } else {
      this.countDownLatchUpdate.countDown();
    }
  }
}
