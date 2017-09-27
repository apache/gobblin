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

package org.apache.gobblin.runtime_constructs;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.runtime.runtime_constructs.DatasetStateStoreSource;
import org.junit.Test;

import static org.apache.gobblin.metastore.DatasetStoreDatasetFinder.*;


/**
 * The unit test will be conducted for reading dataset from filesystem-statestore
 * and writing into filesystem-statestore as well.
 */
public class DatasetStateStoreSourceTest {

  public static String jstPath = "/gobblin-runtime/src/test/resources/store.TestJob/current.jst";
  @Test
  public void testDatasetStateStoreSource() {
    // Specify the path that contains .jst file
    DatasetStateStoreSource datasetStateStoreSource = new DatasetStateStoreSource();
    SourceState sourceState = new SourceState();
    sourceState.setProp(STORE_NAME_FILTER, jstPath);
    datasetStateStoreSource.getWorkunits(sourceState);

  }

  // Tear Down method to clean new dataset-level .jst files
  protected void tearDown() throws Exception {

  }
}
