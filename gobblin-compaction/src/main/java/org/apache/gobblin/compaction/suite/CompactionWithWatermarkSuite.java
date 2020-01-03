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

package org.apache.gobblin.compaction.suite;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.gobblin.compaction.action.CompactionCompleteAction;
import org.apache.gobblin.compaction.action.CompactionCompleteFileOperationAction;
import org.apache.gobblin.compaction.action.CompactionHiveRegistrationAction;
import org.apache.gobblin.compaction.action.CompactionMarkDirectoryAction;
import org.apache.gobblin.compaction.action.CompactionWatermarkAction;
import org.apache.gobblin.compaction.verify.CompactionThresholdVerifier;
import org.apache.gobblin.compaction.verify.CompactionTimeRangeVerifier;
import org.apache.gobblin.compaction.verify.CompactionVerifier;
import org.apache.gobblin.compaction.verify.CompactionWatermarkChecker;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;


/**
 * Compaction suite with watermark checking and publishing for file system dataset of
 * path pattern, [path prefix]/[dataset name]/[partition prefix]/yyyy/MM/[dd/HH/mm], for
 * example:
 * <ul>
 *   <li> home/event1/hourly/2019/12/31 </li>
 *   <li> home/event2/hourly/2019/12/31/10 </li>
 *   <li> home/dbName/tableName/hourly/2019/12/31 </li>
 * </ul>
 *
 * The watermarks are published to hive metastore
 */
public class CompactionWithWatermarkSuite extends CompactionSuiteBase {
  /**
   * Constructor
   * @param state
   */
  public CompactionWithWatermarkSuite(State state) {
    super(state);
  }

  @Override
  public List<CompactionVerifier<FileSystemDataset>> getDatasetsFinderVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new LinkedList<>();
    list.add(new CompactionTimeRangeVerifier(state));
    list.add(new CompactionThresholdVerifier(state));
    return list;
  }

  @Override
  public List<CompactionVerifier<FileSystemDataset>> getMapReduceVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new LinkedList<>();
    list.add(new CompactionWatermarkChecker(state));
    return list;
  }

  @Override
  public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() {
    ArrayList<CompactionCompleteAction<FileSystemDataset>> array = new ArrayList<>();
    array.add(new CompactionCompleteFileOperationAction(state, getConfigurator()));
    array.add(new CompactionHiveRegistrationAction(state));
    // Publish compaction watermarks right after hive registration
    array.add(new CompactionWatermarkAction(state));
    array.add(new CompactionMarkDirectoryAction(state, getConfigurator()));
    return array;
  }
}
