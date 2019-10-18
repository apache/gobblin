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

package org.apache.gobblin.data.management.retention;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.dataset.finder.TimeBasedStateStoreDatasetFinder;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStoreDataset;


/**
 * Unit test for {@link org.apache.gobblin.data.management.retention.state.CleanableStateStoreDataset}
 */
public class CleanableStateStoreDatasetTest {
  @Test
  public void testCleanStateStore() throws IOException {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    FsStateStore<State> store = new FsStateStore<>(fs, tmpDir.getAbsolutePath(), State.class);

    store.put("store_name1", "table_name1", new State());
    store.put("store_name2", "table_name2", new State());
    store.put("store_name3", "table_name3", new State());
    store.put("store_name4", "table_name4", new State());

    Properties props = new Properties();

    props.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, tmpDir.getAbsolutePath());
    props.setProperty("selection.timeBased.lookbackTime", "0m");

    TimeBasedStateStoreDatasetFinder datasetFinder = new TimeBasedStateStoreDatasetFinder(props);
    List<StateStoreDataset> datasets = datasetFinder.findStates();

    for (StateStoreDataset dataset : datasets) {
      ((CleanableDataset) dataset).clean();
      File jobDir = new File(tmpDir.getAbsolutePath(), dataset.getKey().getStoreName());
      Assert.assertEquals(jobDir.list().length, 0);
    }
  }
}
