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
package gobblin.compliance.restore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HivePartitionFinder;
import gobblin.configuration.State;
import gobblin.util.WriterUtils;


/**
 * A finder class for finding Restorable {@link HivePartitionDataset}
 *
 * @author adsharma
 */
@Slf4j
public class RestorableHivePartitionDatasetFinder extends HivePartitionFinder {
  protected FileSystem fs;

  public RestorableHivePartitionDatasetFinder(State state)
      throws IOException {
    this(WriterUtils.getWriterFs(new State(state)), state);
  }

  public RestorableHivePartitionDatasetFinder(FileSystem fs, State state)
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
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RESTORE_DATASET));
    HivePartitionDataset hivePartitionDataset =
        HivePartitionFinder.findDataset(this.state.getProp(ComplianceConfigurationKeys.RESTORE_DATASET), this.state);
    list.add(hivePartitionDataset);
    Preconditions.checkArgument(!list.isEmpty(), "No dataset to restore");
    return list;
  }
}
