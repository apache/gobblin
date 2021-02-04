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
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;


@Test
public class TimePartitionedStreamingDataPublisherTest {
  File tmpDir = Files.createTempDir();
  File publishDir = new File(tmpDir, "/publish");

  /**
   * Test when publish output dir does not exist,
   * it can still record the PublishOutputDirs in right format
   * @throws IOException
   */
  public void testPublishMultiTasks()
      throws IOException {
    WorkUnitState state1 = buildTaskState(2);
    WorkUnitState state2 = buildTaskState(2);
    TimePartitionedStreamingDataPublisher publisher = new TimePartitionedStreamingDataPublisher(state1);
    Assert.assertFalse(publishDir.exists());
    publisher.publishData(ImmutableList.of(state1, state2));
    Assert.assertTrue(publisher.getPublishOutputDirs().contains(new Path(
        state1.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + "." + 1)
            + "/namespace/table/hourly/2020/04/01/12")));
    Assert.assertTrue(publisher.getPublishOutputDirs().contains(new Path(
        state1.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + "." + 0)
            + "/namespace/table/hourly/2020/04/01/12")));
    Assert.assertEquals(publisher.getPublishOutputDirs().size(), 2);
  }

  private WorkUnitState buildTaskState(int numBranches) throws IOException{
    WorkUnitState state = new WorkUnitState(WorkUnit.createEmpty(), new State());

    state.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "namespace");
    state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "table");
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, "namespace_table");
    state.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, numBranches);
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, publishDir.toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, tmpDir.toString());
    if (numBranches > 1) {
      for (int i = 0; i < numBranches; i++) {
        state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + "." + i, publishDir.toString() + "/branch" + i);
        state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + "." + i, tmpDir.toString() + "/branch" + i);
        Files.createParentDirs(new File(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR + "." + i)
            + "/namespace/table/hourly/2020/04/01/12/data.avro"));
        new File(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR + "." + i)
            + "/namespace/table/hourly/2020/04/01/12/data.avro").createNewFile();
      }
    }

    return state;
  }
}