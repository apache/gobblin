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

package gobblin.util.dataset;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.configuration.SourceState;
import gobblin.configuration.State;


/**
 * Tests for {@link DatasetUtils}.
 */
@Test(groups = { "gobblin.util.dataset" })
public class DatasetUtilsTest {

  @Test
  public void testGetDatasetSpecificState() {
    String dataset1 = "testDataset1";
    String dataset2 = "testDataset2";
    String dataset3 = "testDataset3";

    String testKey1 = "testKey1";
    String testValue1 = "testValue1";

    SourceState state = new SourceState();
    state.setProp(DatasetUtils.DATASET_SPECIFIC_PROPS, "[{\"dataset\" : \"" + dataset1 + "\", \"" + testKey1 + "\" : \""
        + testValue1 + "\"}, {\"dataset\" : \"" + dataset2 + "\", \"" + testKey1 + "\" : \"" + testValue1 + "\"}]");

    Map<String, State> datasetSpecificStateMap =
        DatasetUtils.getDatasetSpecificProps(Lists.newArrayList(dataset1, dataset3), state);

    State dataset1ExpectedState = new State();
    dataset1ExpectedState.setProp(testKey1, testValue1);

    Assert.assertEquals(datasetSpecificStateMap.get(dataset1), dataset1ExpectedState);
    Assert.assertNull(datasetSpecificStateMap.get(dataset2));
    Assert.assertNull(datasetSpecificStateMap.get(dataset3));
  }

  @Test
  public void testGetDatasetSpecificStateWithRegex() {
    String dataset1 = "testDataset1";
    String dataset2 = "testDataset2";
    String dataset3 = "otherTestDataset1";

    String testKey1 = "testKey1";
    String testValue1 = "testValue1";

    SourceState state = new SourceState();
    state.setProp(DatasetUtils.DATASET_SPECIFIC_PROPS,
        "[{\"dataset\" : \"testDataset.*\", \"" + testKey1 + "\" : \"" + testValue1 + "\"}]");

    Map<String, State> datasetSpecificStateMap =
        DatasetUtils.getDatasetSpecificProps(Lists.newArrayList(dataset1, dataset2, dataset3), state);

    State dataset1ExpectedState = new State();
    dataset1ExpectedState.setProp(testKey1, testValue1);

    State dataset2ExpectedState = new State();
    dataset2ExpectedState.setProp(testKey1, testValue1);

    Assert.assertEquals(datasetSpecificStateMap.get(dataset1), dataset1ExpectedState);
    Assert.assertEquals(datasetSpecificStateMap.get(dataset2), dataset2ExpectedState);
    Assert.assertNull(datasetSpecificStateMap.get(dataset3));
  }
}
