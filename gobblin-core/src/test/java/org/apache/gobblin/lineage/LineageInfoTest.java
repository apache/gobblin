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

package org.apache.gobblin.lineage;

import java.util.Collection;
import java.util.Map;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gobblin.configuration.State;


public class LineageInfoTest {

  @Test
  public void testDatasetLevel () {
    Collection<LineageInfo> collection = null;
    try {
      collection = LineageInfo.load(createTestStates(), LineageInfo.Level.DATASET);
    } catch (LineageException e) {
      Assert.fail(e.toString());
    }

    Assert.assertEquals(1, collection.size());
    LineageInfo info = collection.iterator().next();
    ImmutableMap<String, String> map = info.getLineageMetaData();
    Assert.assertEquals(3, map.size());
    Assert.assertEquals("V1", map.get("K1"));
    Assert.assertEquals("V2", map.get("K2"));
    Assert.assertEquals("V3", map.get("K3"));
  }

  @Test
  public void testBranchLevel () {
    Collection<LineageInfo> collection = null;
    try {
      collection = LineageInfo.load(createTestStates(), LineageInfo.Level.BRANCH);
    } catch (LineageException e) {
      Assert.fail(e.toString());
    }

    Assert.assertEquals(2, collection.size());

    for (LineageInfo info: collection) {
      Map<String, String> map = info.getLineageMetaData();
      String branchId = map.get(LineageInfo.BRANCH_ID_METADATA_KEY);
      if (branchId.equals("1")) {
        Assert.assertEquals(3, map.size()); // include BRANCH_ID_METADATA_KEY
        Assert.assertEquals("V4", map.get("K4"));
        Assert.assertEquals("V5", map.get("K5"));
      }

      if (branchId.equals("2")) {
        Assert.assertEquals(2, map.size()); // include BRANCH_ID_METADATA_KEY
        Assert.assertEquals("V6", map.get("K6"));
      }
    }
  }

  @Test
  public void testAllLevel () {
    Collection<LineageInfo> collection = null;
    try {
      collection = LineageInfo.load(createTestStates(), LineageInfo.Level.All);
    } catch (LineageException e) {
      Assert.fail(e.toString());
    }

    Assert.assertEquals(2, collection.size());
    for (LineageInfo info: collection) {
      Map<String, String> map = info.getLineageMetaData();
      String branchId = map.get(LineageInfo.BRANCH_ID_METADATA_KEY);
      if (branchId.equals("1")) {
        Assert.assertEquals(6, map.size()); // include BRANCH_ID_METADATA_KEY
        Assert.assertEquals("V1", map.get("K1"));
        Assert.assertEquals("V2", map.get("K2"));
        Assert.assertEquals("V3", map.get("K3"));
        Assert.assertEquals("V4", map.get("K4"));
        Assert.assertEquals("V5", map.get("K5"));
      }

      if (branchId.equals("2")) {
        Assert.assertEquals(5, map.size()); // include BRANCH_ID_METADATA_KEY
        Assert.assertEquals("V1", map.get("K1"));
        Assert.assertEquals("V2", map.get("K2"));
        Assert.assertEquals("V3", map.get("K3"));
        Assert.assertEquals("V6", map.get("K6"));
      }
    }
  }

  @Test
  public void testNoBranchInfo () {
    State state = new State();
    state.setProp(ConfigurationKeys.JOB_ID_KEY, "test_job_id_123456");
    state.setProp(ConfigurationKeys.DATASET_URN_KEY, "PageViewEvent");
    LineageInfo.setDatasetLineageAttribute(state,"K1", "V1");
    LineageInfo.setDatasetLineageAttribute(state,"K2", "V2");
    Collection<LineageInfo> collection = null;
    try {
      collection = LineageInfo.load(Lists.newArrayList(state), LineageInfo.Level.BRANCH);
    } catch (LineageException e) {
      Assert.fail(e.toString());
    }

    Assert.assertEquals(true, collection.isEmpty());
  }

  private Collection<State> createTestStates() {
    /*
     *    State[0]: gobblin.lineage.K1          ---> V1
     *              gobblin.lineage.K2          ---> V2
     *              gobblin.lineage.branch.1.K4 ---> V4
     *    State[1]: gobblin.lineage.K2          ---> V2
     *              gobblin.lineage.K3          ---> V3
     *              gobblin.lineage.branch.1.K4 ---> V4
     *              gobblin.lineage.branch.1.K5 ---> V5
     *              gobblin.lineage.branch.2.K6 ---> V6
     */
    State state_1 = new State();
    state_1.setProp(ConfigurationKeys.JOB_ID_KEY, "test_job_id_123456");
    state_1.setProp(ConfigurationKeys.DATASET_URN_KEY, "PageViewEvent");
    LineageInfo.setDatasetLineageAttribute(state_1,"K1", "V1");
    LineageInfo.setDatasetLineageAttribute(state_1,"K2", "V2");
    LineageInfo.setBranchLineageAttribute(state_1, 1, "K4", "V4");


    State state_2 = new State();
    state_2.setProp(ConfigurationKeys.JOB_ID_KEY, "test_job_id_123456");
    state_2.setProp(ConfigurationKeys.DATASET_URN_KEY, "PageViewEvent");

    LineageInfo.setDatasetLineageAttribute(state_2,"K2", "V2");
    LineageInfo.setDatasetLineageAttribute(state_2,"K3", "V3");
    LineageInfo.setBranchLineageAttribute(state_2, 1, "K4", "V4");
    LineageInfo.setBranchLineageAttribute(state_2, 1, "K5", "V5");
    LineageInfo.setBranchLineageAttribute(state_2, 2, "K6", "V6");

    return Lists.newArrayList(state_1, state_2);
  }
}
