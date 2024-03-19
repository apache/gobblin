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

package org.apache.gobblin.runtime.messaging.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.runtime.messaging.DynamicWorkUnitConsumer;
import org.apache.gobblin.runtime.messaging.hdfs.FileSystemMessageBuffer;

import static org.apache.gobblin.runtime.messaging.DynamicWorkUnitConfigKeys.DYNAMIC_WORKUNIT_HDFS_PATH;
import static org.apache.gobblin.runtime.messaging.DynamicWorkUnitConfigKeys.DYNAMIC_WORKUNIT_HDFS_POLLING_RATE_MILLIS;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;


public class FileSystemMessageBufferTest {

  FileSystemMessageBuffer messageBuffer;
  FileSystem fs;
  File baseDir;
  Path path;

  @BeforeClass
  public void testConstructBuffer()
      throws IOException {
    baseDir = Files.createTempDir();
    this.fs = FileSystem.get(baseDir.toURI(), new Configuration());
    Config cfg = ConfigFactory.empty()
        .withValue(DYNAMIC_WORKUNIT_HDFS_PATH, ConfigValueFactory.fromAnyRef(baseDir.getAbsolutePath()))
        .withValue(DYNAMIC_WORKUNIT_HDFS_POLLING_RATE_MILLIS, ConfigValueFactory.fromAnyRef(500));
    FileSystemMessageBuffer.Factory factory = new FileSystemMessageBuffer.Factory(cfg);
    messageBuffer = (FileSystemMessageBuffer) factory.getBuffer("HDFS_channel");
    path = messageBuffer.getPath();

  }

  @Test
  public void testPublish() throws IOException {
    String workUnitId = "workUnit0";
    List<String> laggingPartitions = new ArrayList<>(Arrays.asList("partition-0","partition-1"));
    SplitWorkUnitMessage splitMessage = new SplitWorkUnitMessage(workUnitId, laggingPartitions);

    messageBuffer.publish(splitMessage);
    fs.exists(path);
    FileStatus[] fileStatus = fs.listStatus(path);
    Assert.assertEquals(fileStatus.length, 1);
    FSDataInputStream fis = fs.open(fileStatus[0].getPath());
    DynamicWorkUnitMessage message = DynamicWorkUnitSerde.deserialize(ByteStreams.toByteArray(fis));

    Assert.assertEquals(message.getWorkUnitId(), workUnitId);
    Assert.assertEquals(((SplitWorkUnitMessage) message).getLaggingTopicPartitions().size(), 2);

  }

  @Test (dependsOnMethods={"testPublish"})
  public void testSubscribe() throws IOException, InterruptedException {
    DynamicWorkUnitConsumer consumer = Mockito.mock(DynamicWorkUnitConsumer.class);

    messageBuffer.subscribe(consumer);
    Thread.sleep(1000);
    Mockito.verify(consumer, times(1)).accept(any(List.class));

    // After reading all messages in the path, files should be cleaned up
    Assert.assertEquals(fs.listStatus(path).length, 0);

  }

  @AfterClass
  public void cleanup() throws IOException {
    fs.delete(new Path(baseDir.getPath()), true);
  }

}