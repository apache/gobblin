/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.copy.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyContext;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.data.management.copy.TestCopyableDataset;
import gobblin.util.PathUtils;

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

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishSingleDataset");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));


    TestDatasetManager datasetManager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath",
            ImmutableList.of("a/b, a/c, d/e"));

    datasetManager.createDatasetFiles();

    datasetManager.verifyDoesntExist();

    copyDataPublisher.publishData(datasetManager.getWorkUnitStates());

    datasetManager.verifyExists();

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPublishMultipleDatasets() throws Exception {

    State state = getTestState("testPublishMultipleDatasets");

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishMultipleDatasets");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));

    TestDatasetManager dataset1Manager =
        new TestDatasetManager(testMethodTempPath, state, "dataset1TargetPath",
            ImmutableList.of("a/b, a/c, d/e"));

    dataset1Manager.createDatasetFiles();

    TestDatasetManager dataset2Manager =
        new TestDatasetManager(testMethodTempPath, state, "dataset2TargetPath",
            ImmutableList.of("a/b, a/c, d/e"));

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

    Path testMethodTempPath = new Path(testClassTempPath, "testPublishOverlappingDatasets");

    CopyDataPublisher copyDataPublisher = closer.register(new CopyDataPublisher(state));

    TestDatasetManager dataset1Manager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath", ImmutableList.of("a/b"));

    dataset1Manager.createDatasetFiles();

    TestDatasetManager dataset2Manager =
        new TestDatasetManager(testMethodTempPath, state, "datasetTargetPath/subDir",
            ImmutableList.of("a/c, d/e"));

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
    testClassTempPath = new Path(this.getClass().getClassLoader().getResource("").getFile(), "CopyDataPublisherTest");
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
    private FileSystem fs;
    private CopyableFile copyableFile;

    private void createDatasetFiles() throws IOException {
      // Create writer output files
      Path datasetWriterOutputPath =
          new Path(new Path(writerOutputPath, copyableFile.getDatasetAndPartition(this.metadata).identifier()),
              PathUtils.withoutLeadingSeparator(metadata.getDatasetTargetRoot()));
      for (String path : relativeFilePaths) {
        Path pathToCreate = new Path(datasetWriterOutputPath, path);
        fs.mkdirs(pathToCreate.getParent());
        fs.create(pathToCreate);
      }
    }

    public TestDatasetManager(Path testMethodTempPath, State state, String datasetTargetPath, List<String> relativeFilePaths)
        throws IOException {

      this.fs = FileSystem.getLocal(new Configuration());
      this.copyableDataset =
          new TestCopyableDataset(new Path("origin"));
      this.metadata = new CopyableDatasetMetadata(this.copyableDataset, new Path(testMethodTempPath, datasetTargetPath));
      this.relativeFilePaths = relativeFilePaths;
      this.writerOutputPath = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));

      FileStatus file = new FileStatus(0, false, 0, 0, 0, new Path("/file"));
      this.copyableFile = CopyableFile.builder(FileSystem.getLocal(new Configuration()), file, new Path("/"),
          new CopyConfiguration(new Path("/"), PreserveAttributes.fromMnemonicString(""), new CopyContext())).build();

      fs.mkdirs(testMethodTempPath);
      log.info("Created a temp directory for test at " + testMethodTempPath);

    }

    List<WorkUnitState> getWorkUnitStates() throws IOException {
      List<WorkUnitState> workUnitStates =
          Lists.newArrayList(new WorkUnitState(), new WorkUnitState(), new WorkUnitState());
      for (WorkUnitState wus : workUnitStates) {
        CopySource.serializeCopyableDataset(wus, metadata);
        CopySource.serializeCopyableFiles(wus, Lists.newArrayList(this.copyableFile));
      }
      return workUnitStates;
    }

    void verifyExists() throws IOException {
      for (String fileRelativePath : relativeFilePaths) {
        Path filePublishPath = new Path(metadata.getDatasetTargetRoot(), fileRelativePath);
        Assert.assertEquals(fs.exists(filePublishPath), true);
      }
    }

    void verifyDoesntExist() throws IOException {
      for (String fileRelativePath : relativeFilePaths) {
        Path filePublishPath = new Path(metadata.getDatasetTargetRoot(), fileRelativePath);
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
