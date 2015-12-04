/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.yarn.event.NewJobConfigArrivalEvent;


/**
 * Unit tests for {@link JobConfigurationManager}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.yarn" })
public class JobConfigurationManagerTest {

  private static final int NUM_JOB_CONFIG_FILES = 3;

  private static final String JOB_CONFIG_DIR_NAME = JobConfigurationManagerTest.class.getSimpleName();

  private final File jobConfigFileDir = new File(JOB_CONFIG_DIR_NAME + GobblinYarnConfigurationKeys.TAR_GZ_FILE_SUFFIX);
  private final EventBus eventBus = new EventBus();
  private JobConfigurationManager jobConfigurationManager;

  private final List<Properties> receivedJobConfigs = Lists.newArrayList();
  private final CountDownLatch countDownLatch = new CountDownLatch(NUM_JOB_CONFIG_FILES);

  @BeforeClass
  public void setUp() throws IOException {
    this.eventBus.register(this);

    // Prepare the test job configuration files
    Assert.assertTrue(jobConfigFileDir.mkdirs());
    Closer closer = Closer.create();
    try {
      for (int i = 0; i < NUM_JOB_CONFIG_FILES; i++) {
        File jobConfigFile = new File(this.jobConfigFileDir, "test" + i + ".job");
        Assert.assertTrue(jobConfigFile.createNewFile());
        Properties properties = new Properties();
        properties.setProperty("foo", "bar" + i);
        properties.store(closer.register(
            Files.newWriter(jobConfigFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), "");
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    this.jobConfigurationManager = new JobConfigurationManager(this.eventBus, Optional.of(JOB_CONFIG_DIR_NAME));
    this.jobConfigurationManager.startAsync().awaitRunning();
  }

  @Test
  public void verifyJobConfigs() throws InterruptedException {
    // Wait for all job configs to be received
    this.countDownLatch.await();

    Assert.assertEquals(this.receivedJobConfigs.size(), 3);
    for (int i = 0; i < NUM_JOB_CONFIG_FILES; i++) {
      Assert.assertEquals(this.receivedJobConfigs.get(i).getProperty("foo"), "bar" + i);
    }
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
    this.receivedJobConfigs.add(newJobConfigArrivalEvent.getJobConfig());
    this.countDownLatch.countDown();
  }
}
