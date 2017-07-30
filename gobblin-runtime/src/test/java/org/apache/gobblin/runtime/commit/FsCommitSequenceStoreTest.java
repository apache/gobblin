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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;

import gobblin.commit.CommitSequence;
import gobblin.commit.CommitStep;
import gobblin.commit.FsRenameCommitStep;
import gobblin.configuration.State;
import gobblin.runtime.JobState.DatasetState;
import gobblin.runtime.commit.DatasetStateCommitStep;
import gobblin.runtime.commit.FsCommitSequenceStore;
import gobblin.util.io.GsonInterfaceAdapter;


/**
 * Tests for {@link FsCommitSequenceStore}.
 *
 * @author Ziyang Liu
 */
public class FsCommitSequenceStoreTest {

  private static final Gson GSON = GsonInterfaceAdapter.getGson(CommitStep.class);

  private final String jobName = "test-job";
  private final String datasetUrn = StringUtils.EMPTY;

  private FsCommitSequenceStore store;
  private CommitSequence sequence;

  @BeforeClass
  public void setUp() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    this.store = new FsCommitSequenceStore(fs, new Path("commit-sequence-store-test"));

    State props = new State();
    props.setId("propsId");
    props.setProp("prop1", "valueOfProp1");
    props.setProp("prop2", "valueOfProp2");
    DatasetState datasetState = new DatasetState();

    datasetState.setDatasetUrn(this.datasetUrn);
    datasetState.incrementJobFailures();
    this.sequence = new CommitSequence.Builder().withJobName("testjob").withDatasetUrn("testurn")
        .beginStep(FsRenameCommitStep.Builder.class).from(new Path("/ab/cd")).to(new Path("/ef/gh")).withProps(props)
        .endStep().beginStep(DatasetStateCommitStep.Builder.class).withDatasetUrn(this.datasetUrn)
        .withDatasetState(datasetState).withProps(props).endStep().build();
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.store.delete(this.jobName);
  }

  @Test
  public void testPut() throws IOException {
    tearDown();
    this.store.put(this.jobName, this.datasetUrn, this.sequence);
    Assert.assertTrue(this.store.exists(this.jobName, this.datasetUrn));

    try {
      this.store.put(this.jobName, this.datasetUrn, this.sequence);
      Assert.fail();
    } catch (IOException e) {
      // Expected to catch IOException
    }
  }

  @Test(dependsOnMethods = { "testPut" })
  public void testGet() throws IOException {
    Optional<CommitSequence> sequence2 = this.store.get(this.jobName, this.datasetUrn);
    Assert.assertTrue(sequence2.isPresent());
    Assert.assertEquals(GSON.toJsonTree(sequence2.get()), GSON.toJsonTree(this.sequence));
  }
}
