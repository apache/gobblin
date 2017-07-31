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
package org.apache.gobblin.compliance.restore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionFinder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.WriterUtils;


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
   * Will return a Singleton list of HivePartitionDataset to be restored.
   */
  public List<HivePartitionDataset> findDatasets()
      throws IOException {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RESTORE_DATASET),
        "Missing required property " + ComplianceConfigurationKeys.RESTORE_DATASET);
    HivePartitionDataset hivePartitionDataset =
        HivePartitionFinder.findDataset(this.state.getProp(ComplianceConfigurationKeys.RESTORE_DATASET), this.state);

    Preconditions.checkNotNull(hivePartitionDataset, "No dataset to restore");
    return Collections.singletonList(hivePartitionDataset);
  }
}
