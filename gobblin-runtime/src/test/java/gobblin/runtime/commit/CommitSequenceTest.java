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

package gobblin.runtime.commit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.commit.CommitSequence;
import gobblin.commit.FsRenameCommitStep;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.runtime.JobState.DatasetState;


/**
 * Tests for {@link CommitSequence}.
 *
 * @author Ziyang Liu
 */
@Test(groups = { "gobblin.runtime.commit" })
public class CommitSequenceTest {

  private static final String ROOT_DIR = "commit-sequence-test";

  private FileSystem fs;
  private CommitSequence sequence;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());

    this.fs.delete(new Path(ROOT_DIR), true);

    Path storeRootDir = new Path(ROOT_DIR, "store");

    Path dir1 = new Path(ROOT_DIR, "dir1");
    Path dir2 = new Path(ROOT_DIR, "dir2");

    this.fs.mkdirs(dir1);
    this.fs.mkdirs(dir2);

    Path src1 = new Path(dir1, "file1");
    Path src2 = new Path(dir2, "file2");
    Path dst1 = new Path(dir2, "file1");
    Path dst2 = new Path(dir1, "file2");
    this.fs.createNewFile(src1);
    this.fs.createNewFile(src2);

    DatasetState ds = new DatasetState("job-name", "job-id");
    ds.setDatasetUrn("urn");
    ds.setNoJobFailure();

    State state = new State();
    state.setProp(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, storeRootDir.toString());

    this.sequence = new CommitSequence.Builder().withJobName("testjob").withDatasetUrn("testurn")
        .beginStep(FsRenameCommitStep.Builder.class).from(src1).to(dst1).withProps(state).endStep()
        .beginStep(FsRenameCommitStep.Builder.class).from(src2).to(dst2).withProps(state).endStep()
        .beginStep(DatasetStateCommitStep.Builder.class).withDatasetUrn("urn").withDatasetState(ds).withProps(state)
        .endStep().build();
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.fs.delete(new Path(ROOT_DIR), true);
  }

  @Test
  public void testExecute() throws IOException {
    this.sequence.execute();

    Assert.assertTrue(this.fs.exists(new Path(ROOT_DIR, "dir1/file2")));
    Assert.assertTrue(this.fs.exists(new Path(ROOT_DIR, "dir2/file1")));
    Assert.assertTrue(this.fs.exists(new Path(ROOT_DIR, "store/job-name/urn-job-id.jst")));
    Assert.assertTrue(this.fs.exists(new Path(ROOT_DIR, "store/job-name/urn-current.jst")));
  }

}
