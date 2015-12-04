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
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.runtime.FsDatasetStateStore;
import gobblin.runtime.JobException;
import gobblin.runtime.JobState;


/**
 * Unit tests for {@link GobblinHelixJobLauncher}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.yarn" })
public class GobblinHelixJobLauncherTest {

  private static final int TEST_ZK_PORT = 3184;

  private HelixManager helixManager;

  private FileSystem localFs;

  private Path appWorkDir;

  private String jobName;

  private File jobOutputFile;

  private GobblinHelixJobLauncher gobblinHelixJobLauncher;

  private GobblinWorkUnitRunner gobblinWorkUnitRunner;

  private FsDatasetStateStore fsDatasetStateStore;

  private Thread thread;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    this.closer.register(new TestingServer(TEST_ZK_PORT));

    URL url = GobblinHelixJobLauncherTest.class.getClassLoader().getResource(
        GobblinHelixJobLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();

    String zkConnectingString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    YarnHelixUtils.createGobblinYarnHelixCluster(zkConnectingString, helixClusterName);

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER,
            zkConnectingString);
    this.helixManager.connect();

    Properties properties = YarnHelixUtils.configToProperties(config);

    this.localFs = FileSystem.getLocal(new Configuration());

    this.appWorkDir = new Path(GobblinHelixJobLauncherTest.class.getSimpleName());

    this.jobName = config.getString(ConfigurationKeys.JOB_NAME_KEY);

    this.jobOutputFile = new File(config.getString(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
        config.getString(ConfigurationKeys.WRITER_FILE_PATH) + File.separator + config
            .getString(ConfigurationKeys.WRITER_FILE_NAME));

    // Prepare the source Json file
    File sourceJsonFile = new File(this.appWorkDir.toString(), TestHelper.TEST_JOB_NAME + ".json");
    TestHelper.createSourceJsonFile(sourceJsonFile);
    properties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, sourceJsonFile.getAbsolutePath());

    this.gobblinHelixJobLauncher = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.localFs, this.appWorkDir,
            ImmutableMap.<String, String>of()));

    this.gobblinWorkUnitRunner = new GobblinWorkUnitRunner(TestHelper.TEST_APPLICATION_NAME,
        ConverterUtils.toContainerId(TestHelper.TEST_PARTICIPANT_CONTAINER_ID), config, Optional.of(appWorkDir));

    this.fsDatasetStateStore =
        new FsDatasetStateStore(this.localFs, config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY));

    this.thread = new Thread(new Runnable() {
      @Override
      public void run() {
        gobblinWorkUnitRunner.start();
      }
    });
    this.thread.start();
  }

  public void testLaunchJob() throws JobException, IOException {
    this.gobblinHelixJobLauncher.launchJob(null);

    Assert.assertTrue(this.jobOutputFile.exists());

    Schema schema = new Schema.Parser().parse(TestHelper.SOURCE_SCHEMA);

    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(this.jobOutputFile, new GenericDatumReader<GenericRecord>(schema))) {
      Iterator<GenericRecord> iterator = reader.iterator();

      GenericRecord record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Alyssa");

      record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Ben");

      record = iterator.next();
      Assert.assertEquals(record.get("name").toString(), "Charlie");

      Assert.assertFalse(iterator.hasNext());
    }

    List<JobState.DatasetState> datasetStates = this.fsDatasetStateStore.getAll(this.jobName,
        FsDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + FsDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX);
    Assert.assertEquals(datasetStates.size(), 1);
    JobState.DatasetState datasetState = datasetStates.get(0);
    Assert.assertEquals(datasetState.getCompletedTasks(), 1);
    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);

    Assert.assertEquals(datasetState.getTaskStates().size(), 1);
    Assert.assertEquals(datasetState.getTaskStates().get(0).getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      this.gobblinWorkUnitRunner.stop();
      this.thread.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    try {
      this.helixManager.disconnect();
      if (this.localFs.exists(this.appWorkDir)) {
        this.localFs.delete(this.appWorkDir, true);
      }
    } finally {
      this.closer.close();
    }
  }
}
