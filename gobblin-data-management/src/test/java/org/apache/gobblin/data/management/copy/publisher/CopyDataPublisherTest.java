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
package org.apache.gobblin.data.management.copy.publisher;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.data.management.copy.TestCopyableDataset;
import org.apache.gobblin.util.PathUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;


/*
 *
 *  Test cases covered
 * - Single dataset multiple files/workunits
 * - Single dataset multiple files/workunits few workunits failed
 * - Two datasets multiple files
 * - Two datasets one of them failed to publish
 * - datasets with overlapping dataset roots
 *
 */
@Slf4j
public class CopyDataPublisherTest {

  private static final Closer closer = Closer.create();

  private FileSystem fs;
  private Path testClassTempPath;

  @Test
  public void testPublishSingleDataset() throws Exception {

    State state = getTestState("testPublishSingleDataset");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishSingleDataset");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));


    TestDatasetManager datasetManager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath",
            ImmutableList.of("a/b", "a/c", "d/e"));

    datasetManager.createDatasetFiles();

    datasetManager.verifyDoesntExist();

    copyDataPublisher.publishData(datasetManager.getWorkUnitStates());

    datasetManager.verifyExists();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPublishMultipleDatasets() throws Exception {

    State state = getTestState("testPublishMultipleDatasets");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishMultipleDatasets");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));

    TestDatasetManager dataset1Manager =
        new TestDatasetManager(testMethodTempPath, state, "dataset1TargetPath",
            ImmutableList.of("a/b", "a/c", "d/e"));

    dataset1Manager.createDatasetFiles();

    TestDatasetManager dataset2Manager =
        new TestDatasetManager(testMethodTempPath, state, "dataset2TargetPath",
            ImmutableList.of("a/b", "a/c", "d/e"));

    dataset2Manager.createDatasetFiles();

    dataset1Manager.verifyDoesntExist();

    dataset2Manager.verifyDoesntExist();

    copyDataPublisher.publishData(combine(dataset1Manager.getWorkUnitStates(), dataset2Manager.getWorkUnitStates()));

    dataset1Manager.verifyExists();

    dataset2Manager.verifyExists();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPublishOverlappingDatasets() throws Exception {

    State state = getTestState("testPublishOverlappingDatasets");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishOverlappingDatasets");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));

    TestDatasetManager dataset1Manager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath", ImmutableList.of("a/b"));

    dataset1Manager.createDatasetFiles();

    TestDatasetManager dataset2Manager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath/subDir",
            ImmutableList.of("a/c", "d/e"));

    dataset2Manager.createDatasetFiles();

    dataset1Manager.verifyDoesntExist();

    dataset2Manager.verifyDoesntExist();

    copyDataPublisher.publishData(combine(dataset1Manager.getWorkUnitStates(), dataset2Manager.getWorkUnitStates()));

    dataset1Manager.verifyExists();

    dataset2Manager.verifyExists();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPublishDatasetFailure() throws Exception {

    State state = getTestState("testPublishDatasetFailure");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishDatasetFailure");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));

    TestDatasetManager successDatasetManager =
        new TestDatasetManager(testMethodTempPath, state, "successTargetPath", ImmutableList.of("a/b"));

    successDatasetManager.createDatasetFiles();

    TestDatasetManager failedDatasetManager =
        new TestDatasetManager(testMethodTempPath, state, "failedTargetPath", ImmutableList.of("c/d"));

    successDatasetManager.verifyDoesntExist();

    failedDatasetManager.verifyDoesntExist();

    copyDataPublisher.publishData(combine(successDatasetManager.getWorkUnitStates(),
        failedDatasetManager.getWorkUnitStates()));

    successDatasetManager.verifyExists();

    failedDatasetManager.verifyDoesntExist();

  }

  @BeforeClass
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    testClassTempPath = new Path(Files.createTempDir().getAbsolutePath(), "CopyDataPublisherTest");
    fs.delete(testClassTempPath, true);
    log.info("Created a temp directory for CopyDataPublisherTest at " + testClassTempPath);
    fs.mkdirs(testClassTempPath);
  }

  private static Collection<? extends WorkUnitState> combine(List<WorkUnitState>... workUnitStateLists) {
    List<WorkUnitState> wus = Lists.newArrayList();
    for (List<WorkUnitState> workUnitStates : workUnitStateLists) {
      wus.addAll(workUnitStates);
    }

    return wus;
  }

  private State getTestState(String testMethodName) {
    return getTestState(testMethodName, testClassTempPath);
  }

  public static State getTestState(String testMethodName, Path testClassTempPath) {

    Path testMethodPath = new Path(testClassTempPath, testMethodName);
    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testMethodPath, "task-output"));
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testMethodPath, "task-staging"));
    state.setProp(ConfigurationKeys.JOB_ID_KEY, "jobid");

    return state;
  }

  public static class TestDatasetManager {

    private CopyableDataset copyableDataset;
    private CopyableDatasetMetadata metadata;
    private List<String> relativeFilePaths;
    private Path writerOutputPath;
    private Path targetPath;
    private FileSystem fs;
    private CopyEntity copyEntity;

    private void createDatasetFiles() throws IOException {
      // Create writer output files
      Path datasetWriterOutputPath =
          new Path(writerOutputPath, copyEntity.getDatasetAndPartition(this.metadata).identifier());
      Path outputPathWithCurrentDirectory = new Path(datasetWriterOutputPath,
          PathUtils.withoutLeadingSeparator(this.targetPath));
      for (String path : relativeFilePaths) {
        Path pathToCreate = new Path(outputPathWithCurrentDirectory, path);
        fs.mkdirs(pathToCreate.getParent());
        fs.create(pathToCreate);
      }
    }

    public TestDatasetManager(Path testMethodTempPath, State state, String datasetTargetPath, List<String> relativeFilePaths)
        throws IOException {

      this.fs = FileSystem.getLocal(new Configuration());
      this.copyableDataset =
          new TestCopyableDataset(new Path("origin"));
      this.metadata = new CopyableDatasetMetadata(this.copyableDataset);
      this.relativeFilePaths = relativeFilePaths;
      this.writerOutputPath = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));

      this.targetPath = new Path(testMethodTempPath, datasetTargetPath);

      FileStatus file = new FileStatus(0, false, 0, 0, 0, new Path("/file"));
      FileSystem fs = FileSystem.getLocal(new Configuration());
      this.copyEntity = CopyableFile.fromOriginAndDestination(fs, file, new Path("/destination"),
          CopyConfiguration.builder(fs, state.getProperties()).preserve(PreserveAttributes.fromMnemonicString(""))
              .build()).build();

      fs.mkdirs(testMethodTempPath);
      log.info("Created a temp directory for test at " + testMethodTempPath);

    }

    List<WorkUnitState> getWorkUnitStates() throws IOException {
      List<WorkUnitState> workUnitStates =
          Lists.newArrayList(new WorkUnitState(), new WorkUnitState(), new WorkUnitState());
      for (WorkUnitState wus : workUnitStates) {
        CopySource.serializeCopyableDataset(wus, metadata);
        CopySource.serializeCopyEntity(wus, this.copyEntity);
      }
      return workUnitStates;
    }

    void verifyExists() throws IOException {
      for (String fileRelativePath : relativeFilePaths) {
        Path filePublishPath = new Path(this.targetPath, fileRelativePath);
        Assert.assertEquals(fs.exists(filePublishPath), true);
      }
    }

    void verifyDoesntExist() throws IOException {
      for (String fileRelativePath : relativeFilePaths) {
        Path filePublishPath = new Path(this.targetPath, fileRelativePath);
        Assert.assertEquals(fs.exists(filePublishPath), false);
      }
    }
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

}
