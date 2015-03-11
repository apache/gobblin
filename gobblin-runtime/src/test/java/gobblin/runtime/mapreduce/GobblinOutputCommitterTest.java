/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;


/**
 * Unit tests for {@link GobblinOutputFormat}
 */
@Test(groups = { "gobblin.runtime.mapreduce" })
public class GobblinOutputCommitterTest {

  private Configuration conf;
  private FileSystem fs;
  private List<Path> stagingDirs;

  private static final Path OUTPUT_PATH = new Path("gobblin-test/output-format-test");

  private static final String STAGING_DIR_NAME = "staging";
  private static final String OUTPUT_DIR_NAME = "output";
  private static final String JOB_NAME = "GobblinOutputFormatTest";

  @BeforeClass
  public void setupWorkUnitFiles() throws IOException {
    this.conf = new Configuration();
    this.fs = FileSystem.getLocal(this.conf);
    this.stagingDirs = Lists.newArrayList();

    // Create a list of WorkUnits to serialize
    WorkUnit wu1 = createAndSetWorkUnit("wu1");
    WorkUnit wu2 = createAndSetWorkUnit("wu2");
    WorkUnit wu3 = createAndSetWorkUnit("wu3");
    WorkUnit wu4 = createAndSetWorkUnit("wu4");

    // Create a MultiWorkUnit to serialize
    MultiWorkUnit mwu1 = new MultiWorkUnit();
    mwu1.setProp(ConfigurationKeys.TASK_ID_KEY, System.nanoTime());
    mwu1.addWorkUnits(Arrays.asList(wu3, wu4));

    Path inputDir = new Path(new Path(OUTPUT_PATH, JOB_NAME), "input");

    // Writer each WorkUnit to a separate file under inputDir
    Closer closer = Closer.create();
    try {
      wu1.write(closer.register(this.fs.create(new Path(inputDir, wu1.getProp(ConfigurationKeys.TASK_ID_KEY)
          + Path.SEPARATOR + "_").suffix("wu"))));
      wu2.write(closer.register(this.fs.create(new Path(inputDir, wu2.getProp(ConfigurationKeys.TASK_ID_KEY)
          + Path.SEPARATOR + "_").suffix("wu"))));
      mwu1.write(closer.register(this.fs.create(new Path(inputDir, mwu1.getProp(ConfigurationKeys.TASK_ID_KEY)
          + Path.SEPARATOR + "_").suffix("mwu"))));
    } finally {
      closer.close();
    }
  }

  @Test()
  public void testAbortJob() throws IOException {
    // Make sure all the staging dirs have been created
    for (Path stagingDir : this.stagingDirs) {
      Assert.assertTrue(this.fs.exists(stagingDir));
    }

    // Cleanup the staging dirs
    Configuration conf = new Configuration();
    conf.set(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
    conf.set(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY, OUTPUT_PATH.toString());
    conf.set(ConfigurationKeys.JOB_NAME_KEY, JOB_NAME);

    new GobblinOutputCommitter().abortJob(Job.getInstance(conf), JobStatus.State.RUNNING);

    // Make sure all the staging dirs have been deleted
    for (Path stagingDir : this.stagingDirs) {
      Assert.assertTrue(!this.fs.exists(stagingDir));
    }
  }

  /**
   * Helper method to create a {@link WorkUnit}, set it's staging directories, and create the staging directories on the
   * local fs
   * @param workUnitName is the name of the {@link WorkUnit} to create
   * @return the {@link WorkUnit} that was created
   * @throws IOException
   */
  private WorkUnit createAndSetWorkUnit(String workUnitName) throws IOException {
    WorkUnit wu = new WorkUnit();
    wu.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.TASK_ID_KEY, 1, 0), System.nanoTime());

    Path wuStagingDir =
        new Path(OUTPUT_PATH, JOB_NAME + Path.SEPARATOR + workUnitName + Path.SEPARATOR + STAGING_DIR_NAME);
    wu.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, 1, 0),
        wuStagingDir.toString());
    this.fs.mkdirs(wuStagingDir);
    this.stagingDirs.add(wuStagingDir);

    Path wuOutputDir =
        new Path(OUTPUT_PATH, JOB_NAME + Path.SEPARATOR + workUnitName + Path.SEPARATOR + OUTPUT_DIR_NAME);
    wu.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 1, 0),
        wuOutputDir.toString());
    this.fs.mkdirs(wuOutputDir);
    this.stagingDirs.add(wuOutputDir);
    return wu;
  }

  @AfterClass
  public void deleteWorkUnitFiles() throws IOException {
    this.fs.delete(OUTPUT_PATH, true);
  }
}
