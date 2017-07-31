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
package org.apache.gobblin.compliance.retention;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionFinder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.WriterUtils;


/**
 * A dataset finder class to find all the {@link CleanableHivePartitionDataset} based on the whitelist.
 *
 * @author adsharma
 */
public class CleanableHivePartitionDatasetFinder extends HivePartitionFinder {
  protected FileSystem fs;

  public CleanableHivePartitionDatasetFinder(State state)
      throws IOException {
    this(WriterUtils.getWriterFs(new State(state)), state);
  }

  public CleanableHivePartitionDatasetFinder(FileSystem fs, State state)
      throws IOException {
    super(state);
    this.fs = fs;
  }

  /**
   * Will find all datasets according to whitelist, except the backup and staging tables.
   */
  public List<HivePartitionDataset> findDatasets()
      throws IOException {
    List<HivePartitionDataset> list = new ArrayList<>();
    for (HivePartitionDataset hivePartitionDataset : super.findDatasets()) {
      CleanableHivePartitionDataset dataset =
          new CleanableHivePartitionDataset(hivePartitionDataset, this.fs, this.state);
      list.add(dataset);
    }
    return list;
  }
}
