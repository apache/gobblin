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
package gobblin.data.management.copy.publisher;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.TestCopyableDataset;

import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.io.Files;


@Slf4j
public class DeletingCopyDataPublisherTest {

  @Test
  public void testDeleteOnSource() throws Exception {

    State state = getTestState("testDeleteOnSource");

    Path testMethodTempPath = new Path(testClassTempPath, "testDeleteOnSource");

    DeletingCopyDataPublisher copyDataPublisher = closer.register(new DeletingCopyDataPublisher(state));

    File outputDir = new File(testMethodTempPath.toString(), "task-output/jobid/1f042f494d1fe2198e0e71a17faa233f33b5099b");
    outputDir.mkdirs();
    outputDir.deleteOnExit();

    WorkUnitState wus = new WorkUnitState();

    CopyableDataset copyableDataset = new TestCopyableDataset(new Path("origin"));
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(copyableDataset);

    CopyEntity cf = CopyableFileUtils.createTestCopyableFile(new Path(testMethodTempPath, "test.txt").toString());

    CopySource.serializeCopyableDataset(wus, metadata);

    CopySource.serializeCopyEntity(wus, cf);

    Assert.assertTrue(fs.exists(new Path(testMethodTempPath, "test.txt")));

    wus.setWorkingState(WorkingState.SUCCESSFUL);

    copyDataPublisher.publishData(ImmutableList.of(wus));

    Assert.assertFalse(fs.exists(new Path(testMethodTempPath, "test.txt")));

  }

  private static final Closer closer = Closer.create();

  private FileSystem fs;
  private Path testClassTempPath;

  @BeforeClass
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    testClassTempPath =
        new Path(Files.createTempDir().getAbsolutePath(), "DeletingCopyDataPublisherTest");
    fs.delete(testClassTempPath, true);
    log.info("Created a temp directory for CopyDataPublisherTest at " + testClassTempPath);
    fs.mkdirs(testClassTempPath);
  }

  @AfterClass
  public void cleanup() {
    try {
      closer.close();
      fs.delete(testClassTempPath, true);
    } catch (IOException e) {
      log.warn(e.getMessage());
    }
  }

  private State getTestState(String testMethodName) {
    return CopyDataPublisherTest.getTestState(testMethodName, testClassTempPath);
  }

}
