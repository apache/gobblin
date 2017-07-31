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

package org.apache.gobblin.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link JobLauncherUtils}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.util" })
public class JobLauncherUtilsTest {

  private static final String JOB_NAME = "foo";
  private static final Pattern PATTERN = Pattern.compile("job_" + JOB_NAME + "_\\d+");
  private String jobId;

  @Test
  public void testNewJobId() {
    this.jobId = JobLauncherUtils.newJobId(JOB_NAME);
    Assert.assertTrue(PATTERN.matcher(this.jobId).matches());
  }

  @Test(dependsOnMethods = "testNewJobId")
  public void testNewTaskId() {
    Assert.assertEquals(JobLauncherUtils.newTaskId(this.jobId, 0), this.jobId.replace("job", "task") + "_0");
    Assert.assertEquals(JobLauncherUtils.newTaskId(this.jobId, 1), this.jobId.replace("job", "task") + "_1");
  }

  @Test(dependsOnMethods = "testNewJobId")
  public void testNewMultiTaskId() {
    Assert.assertEquals(JobLauncherUtils.newMultiTaskId(this.jobId, 0), this.jobId.replace("job", "multitask") + "_0");
    Assert.assertEquals(JobLauncherUtils.newMultiTaskId(this.jobId, 1), this.jobId.replace("job", "multitask") + "_1");
  }

  @Test
  public void testFlattenWorkUnits() {
    List<WorkUnit> workUnitsOnly =
        Arrays.asList(WorkUnit.createEmpty(), WorkUnit.createEmpty(), WorkUnit.createEmpty());

    Assert.assertEquals(JobLauncherUtils.flattenWorkUnits(workUnitsOnly).size(), 3);

    MultiWorkUnit multiWorkUnit1 = MultiWorkUnit.createEmpty();
    multiWorkUnit1.addWorkUnits(Arrays.asList(WorkUnit.createEmpty(), WorkUnit.createEmpty(), WorkUnit.createEmpty()));

    MultiWorkUnit multiWorkUnit2 = MultiWorkUnit.createEmpty();
    multiWorkUnit1.addWorkUnits(Arrays.asList(WorkUnit.createEmpty(), WorkUnit.createEmpty(), WorkUnit.createEmpty()));

    List<WorkUnit> workUnitsAndMultiWorkUnits = Arrays.asList(WorkUnit.createEmpty(), WorkUnit.createEmpty(),
        WorkUnit.createEmpty(), multiWorkUnit1, multiWorkUnit2);

    Assert.assertEquals(JobLauncherUtils.flattenWorkUnits(workUnitsAndMultiWorkUnits).size(), 9);
  }

  @Test
  public void testDeleteStagingData() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());

    Path rootDir = new Path("gobblin-test/job-launcher-utils-test");
    Path writerStagingDir0 = new Path(rootDir, "staging/fork_0");
    Path writerStagingDir1 = new Path(rootDir, "staging/fork_1");
    Path writerOutputDir0 = new Path(rootDir, "output/fork_0");
    Path writerOutputDir1 = new Path(rootDir, "output/fork_1");

    String writerPath0 = "test0";
    String writerPath1 = "test1";

    try {
      WorkUnitState state = new WorkUnitState();
      state.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, 2, 0),
          ConfigurationKeys.LOCAL_FS_URI);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, 2, 1),
          ConfigurationKeys.LOCAL_FS_URI);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, 2, 0),
          writerStagingDir0.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, 2, 1),
          writerStagingDir1.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 2, 0),
          writerOutputDir0.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 2, 1),
          writerOutputDir1.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, 2, 0), writerPath0);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, 2, 1), writerPath1);

      Path writerStagingPath0 = new Path(writerStagingDir0, writerPath0);
      fs.mkdirs(writerStagingPath0);
      Path writerStagingPath1 = new Path(writerStagingDir1, writerPath1);
      fs.mkdirs(writerStagingPath1);
      Path writerOutputPath0 = new Path(writerOutputDir0, writerPath0);
      fs.mkdirs(writerOutputPath0);
      Path writerOutputPath1 = new Path(writerOutputDir1, writerPath1);
      fs.mkdirs(writerOutputPath1);

      JobLauncherUtils.cleanTaskStagingData(state, LoggerFactory.getLogger(JobLauncherUtilsTest.class));

      Assert.assertFalse(fs.exists(writerStagingPath0));
      Assert.assertFalse(fs.exists(writerStagingPath1));
      Assert.assertFalse(fs.exists(writerOutputPath0));
      Assert.assertFalse(fs.exists(writerOutputPath1));
    } finally {
      fs.delete(rootDir, true);
    }
  }

  @Test
  public void testDeleteStagingDataWithOutWriterFilePath() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());

    String branchName0 = "fork_0";
    String branchName1 = "fork_1";

    String namespace = "gobblin.test";
    String tableName = "test-table";

    Path rootDir = new Path("gobblin-test/job-launcher-utils-test");

    Path writerStagingDir0 = new Path(rootDir, "staging" + Path.SEPARATOR + branchName0);
    Path writerStagingDir1 = new Path(rootDir, "staging" + Path.SEPARATOR + branchName1);
    Path writerOutputDir0 = new Path(rootDir, "output" + Path.SEPARATOR + branchName0);
    Path writerOutputDir1 = new Path(rootDir, "output" + Path.SEPARATOR + branchName1);

    try {
      SourceState sourceState = new SourceState();
      WorkUnitState state =
          new WorkUnitState(WorkUnit.create(new Extract(sourceState, TableType.APPEND_ONLY, namespace, tableName)));

      state.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.FORK_BRANCH_NAME_KEY, 2, 0),
          branchName0);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.FORK_BRANCH_NAME_KEY, 2, 1),
          branchName1);

      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, 2, 0),
          ConfigurationKeys.LOCAL_FS_URI);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, 2, 1),
          ConfigurationKeys.LOCAL_FS_URI);
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, 2, 0),
          writerStagingDir0.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, 2, 1),
          writerStagingDir1.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 2, 0),
          writerOutputDir0.toString());
      state.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 2, 1),
          writerOutputDir1.toString());

      Path writerStagingPath0 = new Path(writerStagingDir0,
          ForkOperatorUtils.getPathForBranch(state, state.getExtract().getOutputFilePath(), 2, 0));
      fs.mkdirs(writerStagingPath0);

      Path writerStagingPath1 = new Path(writerStagingDir1,
          ForkOperatorUtils.getPathForBranch(state, state.getExtract().getOutputFilePath(), 2, 1));
      fs.mkdirs(writerStagingPath1);

      Path writerOutputPath0 = new Path(writerOutputDir0,
          ForkOperatorUtils.getPathForBranch(state, state.getExtract().getOutputFilePath(), 2, 0));
      fs.mkdirs(writerOutputPath0);

      Path writerOutputPath1 = new Path(writerOutputDir1,
          ForkOperatorUtils.getPathForBranch(state, state.getExtract().getOutputFilePath(), 2, 1));
      fs.mkdirs(writerOutputPath1);

      JobLauncherUtils.cleanTaskStagingData(state, LoggerFactory.getLogger(JobLauncherUtilsTest.class));

      Assert.assertFalse(fs.exists(writerStagingPath0));
      Assert.assertFalse(fs.exists(writerStagingPath1));
      Assert.assertFalse(fs.exists(writerOutputPath0));
      Assert.assertFalse(fs.exists(writerOutputPath1));
    } finally {
      fs.delete(rootDir, true);
    }
  }
}
