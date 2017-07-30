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

package gobblin.runtime;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.StateStore;
import gobblin.metastore.ZkStateStoreConfigurationKeys;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.source.extractor.extract.LongWatermark;


/**
 * Unit tests for {@link StateStoreBasedWatermarkStorage}.
 **/
@Test(groups = { "gobblin.runtime" })
public class StateStoreWatermarkStorageTest {
  private static final String TEST_JOB_ID = "TestJob1";

  private TestingServer testingServer;
  private long startTime = System.currentTimeMillis();

  @BeforeClass
  public void setUp() throws Exception {
    testingServer = new TestingServer(-1);
  }

  @Test
  public void testPersistWatermarkStateToZk() throws IOException {
    CheckpointableWatermark watermark = new DefaultCheckpointableWatermark("source", new LongWatermark(startTime));

    TaskState taskState = new TaskState();
    taskState.setJobId(TEST_JOB_ID);
    taskState.setProp(ConfigurationKeys.JOB_NAME_KEY, "JobName-" + startTime);
    // watermark storage configuration
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "zk");
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX +
      ZkStateStoreConfigurationKeys.STATE_STORE_ZK_CONNECT_STRING_KEY, testingServer.getConnectString());

    StateStoreBasedWatermarkStorage watermarkStorage = new StateStoreBasedWatermarkStorage(taskState);

    watermarkStorage.commitWatermarks(ImmutableList.of(watermark));
    Map<String, CheckpointableWatermark> watermarkMap = watermarkStorage.getCommittedWatermarks(DefaultCheckpointableWatermark.class,
        ImmutableList.of("source"));

    Assert.assertEquals(watermarkMap.size(), 1);
    Assert.assertEquals(((LongWatermark) watermarkMap.get("source").getWatermark()).getValue(), startTime);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (testingServer != null) {
      testingServer.close();
    }
  }
}