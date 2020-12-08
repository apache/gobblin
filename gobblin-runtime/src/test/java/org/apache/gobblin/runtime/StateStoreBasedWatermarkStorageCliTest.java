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
package org.apache.gobblin.runtime;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.curator.test.TestingServer;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.junit.Before;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StateStoreBasedWatermarkStorageCliTest {

  private static final String TEST_JOB_ID = "TestJob1";

  private TestingServer testingServer;
  private long startTime = System.currentTimeMillis();
  StateStoreBasedWatermarkStorageCli cli;
  PrintStream console;
  ByteArrayOutputStream bytes;

  @BeforeClass
  public void setup() throws Exception {
    this.testingServer = new TestingServer(-1);
    this.bytes = new ByteArrayOutputStream();
    this.console = System.out;
    this.cli = new StateStoreBasedWatermarkStorageCli();
    System.setOut(new PrintStream(this.bytes));
  }

  @AfterMethod
  public void cleanOutput() {
    this.bytes.reset();
  }

  @AfterClass
  public void cleanup() throws IOException {
    this.bytes.close();
    System.setOut(console);
    if (testingServer != null) {
      testingServer.close();
    }
  }

  @Test
  public void testHelpOutput() {
    String[] args = {"watermarks", "--help"};
    cli.run(args);
    Assert.assertTrue(this.bytes.toString().contains("usage: gobblin watermarks\n"));
  }

  @Test
  public void testJobMissingOutput() {
    String[] args = {"watermarks", "--rootDir", "testFolder1"};
    cli.run(args);
    Assert.assertTrue(this.bytes.toString().contains("Need Job Name to be specified --jobName\n"));
  }

  @Test
  public void testMissingRootDirOption() {
    String[] args = {"watermarks", "--jobName", "TestJob1"};
    cli.run(args);
    Assert.assertTrue(this.bytes.toString().contains("Need root directory specified\n"));
  }

  @Test
  public void testViewWatermarks() throws IOException {
    String[] args = {"watermarks", "--jobName", TEST_JOB_ID, "--rootDir", "/streamingWatermarks", "--zk", testingServer.getConnectString()};
    CheckpointableWatermark watermark = new DefaultCheckpointableWatermark("source", new LongWatermark(startTime));

    TaskState taskState = new TaskState();
    taskState.setJobId(TEST_JOB_ID);
    taskState.setProp(ConfigurationKeys.JOB_NAME_KEY, TEST_JOB_ID);
    // watermark storage configuration
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "zk");
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX +
        "state.store.zk.connectString", testingServer.getConnectString());

    StateStoreBasedWatermarkStorage watermarkStorage = new StateStoreBasedWatermarkStorage(taskState);
    watermarkStorage.commitWatermarks(ImmutableList.of(watermark));

    cli.run(args);
    Assert.assertTrue(this.bytes.toString().contains("{source={\"object-type\":\"org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark\",\"object-data\":{\"source\":\"source\",\"comparable\":{\"object-type\":\"org.apache.gobblin.source.extractor.extract.LongWatermark\",\"object-data\":{\"value\":" + this.startTime + "}}}}}\n"));
  }

  @Test(dependsOnMethods="testViewWatermarks")
  public void testDeleteWatermark() {
    String[] args = {"watermarks", "--jobName", TEST_JOB_ID, "--rootDir", "/streamingWatermarks", "--zk", testingServer.getConnectString(), "--delete"};
    CheckpointableWatermark watermark = new DefaultCheckpointableWatermark("source", new LongWatermark(startTime));

    TaskState taskState = new TaskState();
    taskState.setJobId(TEST_JOB_ID);
    taskState.setProp(ConfigurationKeys.JOB_NAME_KEY, TEST_JOB_ID);
    // watermark storage configuration
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "zk");
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX +
        "state.store.zk.connectString", testingServer.getConnectString());

    StateStoreBasedWatermarkStorage watermarkStorage = new StateStoreBasedWatermarkStorage(taskState);

    cli.run(args);
    Assert.assertTrue(this.bytes.toString().contains("No watermarks found."));
  }
}
