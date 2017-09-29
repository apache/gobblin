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
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.dataset.finder.TimeBasedDatasetStoreDatasetFinder;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.runtime.FsDatasetStateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.google.common.io.Files;


/**
 * Unit test for {@link org.apache.gobblin.data.management.retention.dataset.CleanableDatasetStoreDataset}
 */
public class CleanableDatasetStoreDatasetTest {
  @Test
  public void testCleanStateStore() throws IOException {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    FsDatasetStateStore store = new FsDatasetStateStore(fs, tmpDir.getAbsolutePath());

    store.persistDatasetState("dataset1", new JobState.DatasetState("job1", "job1_id1"));
    store.persistDatasetState("dataset1", new JobState.DatasetState("job1", "job1_id2"));
    store.persistDatasetState("dataset1", new JobState.DatasetState("job2", "job2_id1"));
    store.persistDatasetState("", new JobState.DatasetState("job3", "job3_id1"));

    Properties props = new Properties();

    props.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, tmpDir.getAbsolutePath());
    props.setProperty("selection.timeBased.lookbackTime", "0m");

    TimeBasedDatasetStoreDatasetFinder datasetFinder = new TimeBasedDatasetStoreDatasetFinder(fs, props);
    List<DatasetStoreDataset> datasets = datasetFinder.findDatasets();

    for (DatasetStoreDataset dataset : datasets) {
      ((CleanableDataset) dataset).clean();
      File jobDir = new File(tmpDir.getAbsolutePath(), dataset.getKey().getStoreName());
      Assert.assertEquals(jobDir.list().length, 1);
    }
  }
}
