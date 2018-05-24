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
package org.apache.gobblin.publisher;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.gobblin_scopes.TaskScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.broker.iface.SubscopedBrokerBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metadata.MetadataMerger;
import org.apache.gobblin.metadata.types.GlobalMetadata;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.writer.FsDataWriter;
import org.apache.gobblin.writer.FsWriterMetrics;
import org.apache.gobblin.writer.PartitionIdentifier;


/**
 * Tests for BaseDataPublisher
 */
public class BaseDataPublisherTest {
  /**
   * Test DATA_PUBLISHER_METADATA_STR: a user should be able to put an arbitrary metadata string in job configuration
   * and have that written out.
   */
  @Test
  public void testMetadataStrOneBranch()
      throws IOException {
    State s = buildDefaultState(1);

    WorkUnitState wuState = new WorkUnitState();
    wuState.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_STR, "foobar");
    addStateToWorkunit(s, wuState);

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(wuState);

    try (InputStream mdStream = new FileInputStream(openMetadataFile(s, 1, 0))) {
      String mdBytes = IOUtils.toString(mdStream, StandardCharsets.UTF_8);
      Assert.assertEquals(mdBytes, "foobar", "Expected to read back metadata from string");
    }
  }

  /**
   * Test that DATA_PUBLISHER_METADATA_STR functionality works across multiple branches.
   */
  @Test
  public void testMetadataStrMultipleWorkUnitsAndBranches()
      throws IOException {
    final int numBranches = 3;
    State s = buildDefaultState(numBranches);

    List<WorkUnitState> workUnits = new ArrayList<>();
    for (int i = 0; i < numBranches; i++) {
      WorkUnitState wuState = new WorkUnitState();
      wuState.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_STR, "foobar");
      addStateToWorkunit(s, wuState);
      workUnits.add(wuState);
    }

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(workUnits);

    for (int branch = 0; branch < numBranches; branch++) {
      try (InputStream mdStream = new FileInputStream(openMetadataFile(s, numBranches, branch))) {
        String mdBytes = IOUtils.toString(mdStream, StandardCharsets.UTF_8);
        Assert.assertEquals(mdBytes, "foobar", "Expected to read back metadata from string");
      }
    }
  }

  /**
   * Test that an exception is properly thrown if we configure a merger that doesn't actually implement
   * MetadataMerger
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBogusMetadataMerger()
      throws IOException {
    State s = buildDefaultState(1);
    s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, "true");
    s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_KEY, "java.lang.String");
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_STR, "foobar");
    WorkUnitState wuState = new WorkUnitState();
    addStateToWorkunit(s, wuState);

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(Collections.singletonList(wuState));
  }

  /**
   * This test is testing several things at once:
   *  1. That a merger is called properly for all workunits in a brach
   *  2. That different mergers can be instantiated per branch
   */
  @Test
  public void testMergedMetadata()
      throws IOException {
    final int numBranches = 2;
    final int numWorkUnits = 10;

    State s = buildDefaultState(numBranches);

    for (int i = 0; i < numBranches; i++) {
      String mdKeyName = ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, numBranches, i);
      String mdMergerKeyName = ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_KEY, numBranches, i);

      s.setProp(mdKeyName, "true");
      s.setProp(mdMergerKeyName,
          (i % 2) == 0 ? TestAdditionMerger.class.getName() : TestMultiplicationMerger.class.getName());
    }

    // For each branch, metadata is (branchId+1*workUnitNumber+1) - adding 1 so we don't ever multiply by 0
    List<WorkUnitState> workUnits = new ArrayList<>();
    for (int workUnitId = 0; workUnitId < numWorkUnits; workUnitId++) {
      WorkUnitState wuState = new WorkUnitState();
      addStateToWorkunit(s, wuState);

      for (int branchId = 0; branchId < numBranches; branchId++) {
        String mdForBranchName =
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_METADATA_KEY, numBranches, branchId);
        wuState.setProp(mdForBranchName, String.valueOf((branchId + 1) * (workUnitId + 1)));
      }

      workUnits.add(wuState);
    }

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(workUnits);

    for (int branch = 0; branch < numBranches; branch++) {
      int expectedSum = (branch % 2 == 0) ? 0 : 1;
      for (int i = 0; i < numWorkUnits; i++) {
        if (branch % 2 == 0) {
          expectedSum += (branch + 1) * (i + 1);
        } else {
          expectedSum *= (branch + 1) * (i + 1);
        }
      }

      try (InputStream mdStream = new FileInputStream(openMetadataFile(s, numBranches, branch))) {
        String mdBytes = IOUtils.toString(mdStream, StandardCharsets.UTF_8);
        Assert.assertEquals(mdBytes, String.valueOf(expectedSum), "Expected to read back correctly merged metadata from string");
      }
    }
  }

  @Test
  public void testNoOutputWhenDisabled()
      throws IOException {
    State s = buildDefaultState(1);

    WorkUnitState wuState = new WorkUnitState();
    addStateToWorkunit(s, wuState);

    wuState.setProp(ConfigurationKeys.WRITER_METADATA_KEY, "abcdefg");

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(Collections.singletonList(wuState));

    File mdFile = openMetadataFile(s, 1, 0);
    Assert.assertFalse(mdFile.exists(), "Internal metadata from writer should not be written out if no merger is set in config");
  }

  @Test
  public void testNoOutputWhenDisabledWithPartitions()
      throws IOException {

    File publishPath = Files.createTempDir();

    State s = buildDefaultState(1);
    s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
    s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE);
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishPath.getAbsolutePath());

    WorkUnitState wuState = new WorkUnitState();
    addStateToWorkunit(s, wuState);

    wuState.setProp(ConfigurationKeys.WRITER_METADATA_KEY, "abcdefg");

    FsWriterMetrics metrics1 = buildWriterMetrics("foo1.json", "1-2-3-4", 0, 10);
    FsWriterMetrics metrics2 = buildWriterMetrics("foo1.json", "5-6-7-8",10, 20);
    wuState.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "1-2-3-4");
    wuState.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics1.toJson());
    wuState.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + "_0", "1-2-3-4");
    wuState.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + " _0", metrics2.toJson());
    wuState.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + "_1", "5-6-7-8");
    wuState.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + " _1", metrics2.toJson());

    BaseDataPublisher publisher = new BaseDataPublisher(s);
    publisher.publishMetadata(Collections.singletonList(wuState));

    String[] filesInPublishDir = publishPath.list();
    Assert.assertEquals(0, filesInPublishDir.length, "Expected 0 files to be output to publish path");
  }

  @Test
  public void testMergesExistingMetadata() throws IOException {
    File publishPath = Files.createTempDir();
    try {
      // Copy the metadata file from resources into the publish path
      InputStream mdStream = this.getClass().getClassLoader().getResourceAsStream("publisher/sample_metadata.json");
      try (FileOutputStream fOs = new FileOutputStream(new File(publishPath, "metadata.json"))) {
        IOUtils.copy(mdStream, fOs);
      }

      State s = buildDefaultState(1);
      String md = new GlobalMetadata().toJson();

      s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, "true");
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishPath.getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, "metadata.json");

      WorkUnitState wuState1 = new WorkUnitState();
      FsWriterMetrics metrics1 = buildWriterMetrics("newfile.json", null, 0, 90);
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics1.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      addStateToWorkunit(s, wuState1);

      BaseDataPublisher publisher = new BaseDataPublisher(s);
      publisher.publishMetadata(ImmutableList.of(wuState1));

      checkMetadata(new File(publishPath.getAbsolutePath(), "metadata.json"), 4,185,
          new FsWriterMetrics.FileInfo("foo3.json", 30),
          new FsWriterMetrics.FileInfo("foo1.json", 10),
          new FsWriterMetrics.FileInfo("foo4.json", 55),
          new FsWriterMetrics.FileInfo("newfile.json", 90));
    } finally {
      FileUtils.deleteDirectory(publishPath);
    }
  }

  @Test
  public void testWithFsMetricsNoPartitions() throws IOException {
    File publishPath = Files.createTempDir();
    try {
      State s = buildDefaultState(1);
      String md = new GlobalMetadata().toJson();

      s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, "true");
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishPath.getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, "metadata.json");

      WorkUnitState wuState1 = new WorkUnitState();
      FsWriterMetrics metrics1 = buildWriterMetrics("foo1.json", null, 0, 10);
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics1.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      addStateToWorkunit(s, wuState1);

      WorkUnitState wuState2 = new WorkUnitState();
      FsWriterMetrics metrics3 = buildWriterMetrics("foo3.json", null, 1, 30);
      wuState2.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      wuState2.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics3.toJson());
      addStateToWorkunit(s, wuState2);

      WorkUnitState wuState3 = new WorkUnitState();
      FsWriterMetrics metrics4 = buildWriterMetrics("foo4.json", null, 2, 55);
      wuState3.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      wuState3.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics4.toJson());
      addStateToWorkunit(s, wuState3);

      BaseDataPublisher publisher = new BaseDataPublisher(s);
      publisher.publishMetadata(ImmutableList.of(wuState1, wuState2, wuState3));

      checkMetadata(new File(publishPath.getAbsolutePath(), "metadata.json"), 3, 95,
          new FsWriterMetrics.FileInfo("foo3.json", 30),
          new FsWriterMetrics.FileInfo("foo1.json", 10),
          new FsWriterMetrics.FileInfo("foo4.json", 55));
    } finally {
      FileUtils.deleteDirectory(publishPath);
    }
  }

  @Test
  public void testWithFsMetricsAndPartitions() throws IOException {
    File publishPath = Files.createTempDir();
    try {
      File part1 = new File(publishPath, "1-2-3-4");
      part1.mkdir();

      File part2 = new File(publishPath, "5-6-7-8");
      part2.mkdir();

      State s = buildDefaultState(1);
      String md = new GlobalMetadata().toJson();

      s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, "true");
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishPath.getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, "metadata.json");

      WorkUnitState wuState1 = new WorkUnitState();
      FsWriterMetrics metrics1 = buildWriterMetrics("foo1.json", "1-2-3-4", 0, 10);
      FsWriterMetrics metrics2 = buildWriterMetrics("foo1.json", "5-6-7-8",10, 20);
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "1-2-3-4");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics1.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + "_0", "1-2-3-4");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + " _0", metrics2.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + "_1", "5-6-7-8");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + " _1", metrics2.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      addStateToWorkunit(s, wuState1);

      WorkUnitState wuState2 = new WorkUnitState();
      FsWriterMetrics metrics3 = buildWriterMetrics("foo3.json", "1-2-3-4", 1, 30);
      wuState2.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "1-2-3-4");
      wuState2.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      wuState2.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics3.toJson());
      addStateToWorkunit(s, wuState2);

      WorkUnitState wuState3 = new WorkUnitState();
      FsWriterMetrics metrics4 = buildWriterMetrics("foo4.json", "5-6-7-8", 2, 55);
      wuState3.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "5-6-7-8");
      wuState3.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      wuState3.setProp(FsDataWriter.FS_WRITER_METRICS_KEY, metrics4.toJson());
      addStateToWorkunit(s, wuState3);

      BaseDataPublisher publisher = new BaseDataPublisher(s);
      publisher.publishMetadata(ImmutableList.of(wuState1, wuState2, wuState3));

      checkMetadata(new File(part1, "metadata.json"), 2, 40,
          new FsWriterMetrics.FileInfo("foo3.json", 30),
          new FsWriterMetrics.FileInfo("foo1.json", 10));
      checkMetadata(new File(part2, "metadata.json"), 2, 75,
          new FsWriterMetrics.FileInfo("foo1.json", 20),
          new FsWriterMetrics.FileInfo("foo4.json", 55));
    } finally {
      FileUtils.deleteDirectory(publishPath);
    }
  }

  @Test
  public void testWithFsMetricsBranchesAndPartitions() throws IOException {
    File publishPaths[] = new File[] {
        Files.createTempDir(), // branch 0
        Files.createTempDir(), // branch 1
    };

    try {
      List<File[]> branchPaths = Arrays.stream(publishPaths).map(branchPath -> new File[] {
          new File(branchPath, "1-2-3-4"),
          new File(branchPath, "5-6-7-8")
      }).collect(Collectors.toList());

      branchPaths.forEach(partitionPaths -> Arrays.stream(partitionPaths).forEach(File::mkdir));

      State s = buildDefaultState(2);
      String md = new GlobalMetadata().toJson();

      s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY + ".0", "true");
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY + ".1", "true");
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY + ".0", md);
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY + ".1", md);
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0", publishPaths[0].getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1", publishPaths[1].getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR + ".0", "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR + ".1", "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, "metadata.json");

      WorkUnitState wuState1 = new WorkUnitState();
      FsWriterMetrics metrics1 = buildWriterMetrics("foo1.json", "1-2-3-4", 0, 10);
      FsWriterMetrics metrics2 = buildWriterMetrics("foo1.json", "5-6-7-8",10, 20);
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + ".0", "1-2-3-4");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + ".0", metrics1.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + ".0_0", "1-2-3-4");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + ".0_0", metrics2.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + ".0" + "_1", "5-6-7-8");
      wuState1.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + ".0_1", metrics2.toJson());
      wuState1.setProp(ConfigurationKeys.WRITER_METADATA_KEY + ".0", md);
      addStateToWorkunit(s, wuState1);

      WorkUnitState wuState2 = new WorkUnitState();
      FsWriterMetrics metrics3 = buildWriterMetrics("foo3.json", "1-2-3-4", 1, 1, 30);
      wuState2.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + ".1", "1-2-3-4");
      wuState2.setProp(ConfigurationKeys.WRITER_METADATA_KEY + ".1", md);
      wuState2.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + ".1", metrics3.toJson());
      addStateToWorkunit(s, wuState2);

      WorkUnitState wuState3 = new WorkUnitState();
      FsWriterMetrics metrics4 = buildWriterMetrics("foo4.json", "5-6-7-8", 2, 55);
      wuState3.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + ".0", "5-6-7-8");
      wuState3.setProp(ConfigurationKeys.WRITER_METADATA_KEY + ".0", md);
      wuState3.setProp(FsDataWriter.FS_WRITER_METRICS_KEY + ".0", metrics4.toJson());
      addStateToWorkunit(s, wuState3);

      BaseDataPublisher publisher = new BaseDataPublisher(s);
      publisher.publishMetadata(ImmutableList.of(wuState1, wuState2, wuState3));

      checkMetadata(new File(branchPaths.get(0)[0], "metadata.json.0"), 1, 10,
          new FsWriterMetrics.FileInfo("foo1.json", 10));
      checkMetadata(new File(branchPaths.get(0)[1], "metadata.json.0"), 2, 75,
          new FsWriterMetrics.FileInfo("foo1.json", 20),
          new FsWriterMetrics.FileInfo("foo4.json", 55));
      checkMetadata(new File(branchPaths.get(1)[0], "metadata.json.1"), 1, 30,
          new FsWriterMetrics.FileInfo("foo3.json", 30));
    } finally {
      Arrays.stream(publishPaths).forEach(dir -> {
        try {
          FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
          throw new RuntimeException("IOError");
        }
      });
    }
  }

  private void checkMetadata(File file, int expectedNumFiles, int expectedNumRecords,
      FsWriterMetrics.FileInfo... expectedFileInfo)
      throws IOException {
    Assert.assertTrue(file.exists(), "Expected file " + file.getAbsolutePath() + " to exist");
    String contents = IOUtils.toString(new FileInputStream(file), StandardCharsets.UTF_8);
    GlobalMetadata metadata = GlobalMetadata.fromJson(contents);

    Assert.assertEquals(metadata.getNumFiles(), expectedNumFiles, "# of files do not match");
    Assert.assertEquals(metadata.getNumRecords(), expectedNumRecords, "# of records do not match");
    for (FsWriterMetrics.FileInfo fileInfo : expectedFileInfo) {
      long recordsInMetadata =
          ((Number) metadata.getFileMetadata(fileInfo.getFileName(), GlobalMetadata.NUM_RECORDS_KEY)).longValue();
      Assert.assertEquals(recordsInMetadata, fileInfo.getNumRecords(),
          "# of records in file-level metadata do not match");
    }
  }


  private FsWriterMetrics buildWriterMetrics(String fileName, String partitionKey, int writerId, int numRecords) {
    return buildWriterMetrics(fileName, partitionKey, writerId, 0, numRecords);
  }

  private FsWriterMetrics buildWriterMetrics(String fileName, String partitionKey, int writerId, int branchId, int numRecords) {
    return new FsWriterMetrics(
        String.format("writer%d", writerId),
        new PartitionIdentifier(partitionKey, branchId),
        ImmutableList.of(new FsWriterMetrics.FileInfo(fileName, numRecords))
    );
  }

  @Test
  public void testWithPartitionKey() throws IOException {
    File publishPath = Files.createTempDir();
    try {
      File part1 = new File(publishPath, "1-2-3-4");
      part1.mkdir();

      File part2 = new File(publishPath, "5-6-7-8");
      part2.mkdir();

      State s = buildDefaultState(1);
      String md = new GlobalMetadata().toJson();

      s.removeProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      s.setProp(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, "true");
      s.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishPath.getAbsolutePath());
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
      s.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, "metadata.json");

      WorkUnitState wuState1 = new WorkUnitState();
      wuState1.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "1-2-3-4");
      wuState1.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      addStateToWorkunit(s, wuState1);

      WorkUnitState wuState2 = new WorkUnitState();
      wuState2.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY, "5-6-7-8");
      wuState2.setProp(ConfigurationKeys.WRITER_METADATA_KEY, md);
      addStateToWorkunit(s, wuState2);

      BaseDataPublisher publisher = new BaseDataPublisher(s);
      publisher.publishMetadata(ImmutableList.of(wuState1, wuState2));

      Assert.assertTrue(new File(part1, "metadata.json").exists());
      Assert.assertTrue(new File(part2, "metadata.json").exists());
    } finally {
      FileUtils.deleteDirectory(publishPath);
    }
  }

  @Test
  public void testPublishSingleTask()
      throws IOException {
    WorkUnitState state = buildTaskState(1);
    LineageInfo lineageInfo = LineageInfo.getLineageInfo(state.getTaskBroker()).get();
    DatasetDescriptor source = new DatasetDescriptor("kafka", "testTopic");
    lineageInfo.setSource(source, state);
    BaseDataPublisher publisher = new BaseDataPublisher(state);
    publisher.publishData(state);
    Assert.assertTrue(state.contains("gobblin.event.lineage.branch.0.destination"));
    Assert.assertFalse(state.contains("gobblin.event.lineage.branch.1.destination"));
  }

  @Test
  public void testPublishMultiTasks()
      throws IOException {
    WorkUnitState state1 = buildTaskState(2);
    WorkUnitState state2 = buildTaskState(2);
    LineageInfo lineageInfo = LineageInfo.getLineageInfo(state1.getTaskBroker()).get();
    DatasetDescriptor source = new DatasetDescriptor("kafka", "testTopic");
    lineageInfo.setSource(source, state1);
    lineageInfo.setSource(source, state2);
    BaseDataPublisher publisher = new BaseDataPublisher(state1);
    publisher.publishData(ImmutableList.of(state1, state2));
    Assert.assertTrue(state1.contains("gobblin.event.lineage.branch.0.destination"));
    Assert.assertTrue(state1.contains("gobblin.event.lineage.branch.1.destination"));
    Assert.assertTrue(state2.contains("gobblin.event.lineage.branch.0.destination"));
    Assert.assertTrue(state2.contains("gobblin.event.lineage.branch.1.destination"));
  }

  public static class TestAdditionMerger implements MetadataMerger<String> {
    private int sum = 0;

    @Override
    public void update(String metadata) {
      sum += Integer.valueOf(metadata);
    }

    @Override
    public void update(FsWriterMetrics metrics) {

    }

    @Override
    public String getMergedMetadata() {
      return String.valueOf(sum);
    }
  }

  public static class TestMultiplicationMerger implements MetadataMerger<String> {
    private int product = 1;

    public TestMultiplicationMerger(Properties config) {
      // testing ctor call
    }

    @Override
    public void update(String metadata) {
      product *= Integer.valueOf(metadata);
    }

    @Override
    public String getMergedMetadata() {
      return String.valueOf(product);
    }

    @Override
    public void update(FsWriterMetrics metrics) {

    }
  }

  private void addStateToWorkunit(State s, WorkUnitState wuState) {
    for (Map.Entry<Object, Object> prop : s.getProperties().entrySet()) {
      wuState.setProp((String) prop.getKey(), prop.getValue());
    }
  }

  private File openMetadataFile(State state, int numBranches, int branchId) {
    String dir = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
    String fileName = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE);
    if (numBranches > 1) {
      fileName += "." + String.valueOf(branchId);
    }
    return new File(dir, fileName);
  }

  private State buildDefaultState(int numBranches)
      throws IOException {
    State state = new State();

    state.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, numBranches);
    File tmpLocation = File.createTempFile("metadata", "");
    tmpLocation.delete();
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR, tmpLocation.getParent());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE, tmpLocation.getName());

    return state;
  }

  private WorkUnitState buildTaskState(int numBranches) {
    SharedResourcesBroker<GobblinScopeTypes> instanceBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.empty(), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<GobblinScopeTypes> jobBroker = instanceBroker
        .newSubscopedBuilder(new JobScopeInstance("LineageEventTest", String.valueOf(System.currentTimeMillis())))
        .build();
    SharedResourcesBroker<GobblinScopeTypes> taskBroker = jobBroker
        .newSubscopedBuilder(new TaskScopeInstance("LineageEventTestTask" + String.valueOf(System.currentTimeMillis())))
        .build();

    WorkUnitState state = new WorkUnitState(WorkUnit.createEmpty(), new State(), taskBroker);

    state.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "namespace");
    state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "table");
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, "namespace_table");
    state.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, numBranches);
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/data/output");
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, "/data/working");
    if (numBranches > 1) {
      for (int i = 0; i < numBranches; i++) {
        state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + "." + i, "/data/output" + "/branch" + i);
        state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + "." + i, "/data/working" + "/branch" + i);
      }
    }

    return state;
  }
}
