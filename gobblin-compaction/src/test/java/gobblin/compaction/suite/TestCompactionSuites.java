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

package gobblin.compaction.suite;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;

@Slf4j
public class TestCompactionSuites {

  /**
   * Test hive registration failure
   */
  public static class HiveRegistrationCompactionSuite extends CompactionAvroSuite {

    public HiveRegistrationCompactionSuite(State state) {
      super(state);
    }

    public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() {
      ArrayList<CompactionCompleteAction<FileSystemDataset>> array = new ArrayList<>();
      array.add((dataset) -> {
        if (dataset.datasetURN().contains(TestCompactionSuiteFactories.DATASET_FAIL))
          throw new RuntimeException("test-hive-registration-failure");
      });
      return array;
    }
  }
}
