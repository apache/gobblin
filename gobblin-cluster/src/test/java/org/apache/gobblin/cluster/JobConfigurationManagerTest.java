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

package gobblin.cluster;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import gobblin.cluster.event.NewJobConfigArrivalEvent;
import gobblin.configuration.ConfigurationKeys;


/**
 * Unit tests for {@link JobConfigurationManager}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class JobConfigurationManagerTest {

  private static final int NUM_JOB_CONFIG_FILES = 3;

  private static final String JOB_CONFIG_DIR_NAME = JobConfigurationManagerTest.class.getSimpleName();

  private final File jobConfigFileDir = new File(JOB_CONFIG_DIR_NAME + GobblinClusterConfigurationKeys.TAR_GZ_FILE_SUFFIX);
  private final EventBus eventBus = new EventBus();
  private JobConfigurationManager jobConfigurationManager;

  private final List<Properties> receivedJobConfigs = Lists.newArrayList();
  private final CountDownLatch countDownLatch = new CountDownLatch(NUM_JOB_CONFIG_FILES);

  @BeforeClass
  public void setUp() throws IOException {
    this.eventBus.register(this);

    // Prepare the test job configuration files
    Assert.assertTrue(this.jobConfigFileDir.mkdirs(), "Failed to create " + this.jobConfigFileDir);
    Closer closer = Closer.create();
    try {
      for (int i = 0; i < NUM_JOB_CONFIG_FILES; i++) {
        File jobConfigFile = new File(this.jobConfigFileDir, "test" + i + ".job");
        Assert.assertTrue(jobConfigFile.createNewFile());
        Properties properties = new Properties();
        properties.setProperty("foo", "bar" + i);
        properties.store(closer.register(Files.newWriter(jobConfigFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
            "");
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    Config config = ConfigFactory.empty().withValue(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY,
        ConfigValueFactory.fromAnyRef(JOB_CONFIG_DIR_NAME));
    this.jobConfigurationManager = new JobConfigurationManager(this.eventBus, config);
    this.jobConfigurationManager.startAsync().awaitRunning();
  }

  @Test
  public void verifyJobConfigs() throws InterruptedException {
    // Wait for all job configs to be received
    this.countDownLatch.await();

    Set<String> actual = Sets.newHashSet();
    Set<String> expected = Sets.newHashSet();

    Assert.assertEquals(this.receivedJobConfigs.size(), 3);
    for (int i = 0; i < NUM_JOB_CONFIG_FILES; i++) {
      actual.add(this.receivedJobConfigs.get(i).getProperty("foo"));
      expected.add("bar" + i);
    }

    Assert.assertEquals(actual, expected);
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
