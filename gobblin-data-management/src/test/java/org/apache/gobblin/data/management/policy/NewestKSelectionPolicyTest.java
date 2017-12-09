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
package org.apache.gobblin.data.management.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.data.management.version.DatasetVersion;

/** Unit tests for {@link NewestKSelectionPolicy} */
public class NewestKSelectionPolicyTest {

  private static final Map<String, Map<String, Integer>> TEST_CONFIGS =
      ImmutableMap.<String, Map<String, Integer>>builder()
      .put("empty", ImmutableMap.<String, Integer>builder().build())
      .put("selectedPos", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY, 5)
                          .build())
      .put("notSelectedPos", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY, 10)
                          .build())
      .build();

  private static final Map<String, Map<String, Integer>> NEG_TEST_CONFIGS =
      ImmutableMap.<String, Map<String, Integer>>builder()
      .put("bothProps", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY, 5)
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY, 5)
                          .build())
      .put("selectedNeg", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY, -5)
                          .build())
      .put("notSelectedNeg", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY, -1)
                          .build())
      .put("selectedBig", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY,
                               NewestKSelectionPolicy.MAX_VERSIONS_ALLOWED + 1)
                          .build())
      .put("notSelectedBig", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY,
                              NewestKSelectionPolicy.MAX_VERSIONS_ALLOWED + 1)
                          .build())
      .put("selected0", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY, 0)
                          .build())
      .put("notSelected0", ImmutableMap.<String, Integer>builder()
                          .put(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY, 0)
                          .build())
      .build();

  private static final Map<String, Integer> TEST_RESULTS =
      ImmutableMap.<String, Integer>builder()
      .put("empty", NewestKSelectionPolicy.VERSIONS_SELECTED_DEFAULT)
      .put("selectedPos", 5)
      .put("notSelectedPos", -10)
      .build();

  public static class TestStringDatasetVersion implements DatasetVersion,
                                                      Comparable<DatasetVersion> {
    private String _version;

    public TestStringDatasetVersion(String version) {
      _version = version;
    }

    @Override
    public int compareTo(DatasetVersion o) {
      if (!(o instanceof TestStringDatasetVersion)) {
        throw new RuntimeException("Incompatible version: " + o);
      }
      return _version.compareTo(((TestStringDatasetVersion)o)._version);
    }

    @Override
    public Object getVersion() {
      return _version;
    }

  }

  @Test
  public void testCreationProps() {
    for(Map.Entry<String, Map<String, Integer>> test: TEST_CONFIGS.entrySet()) {
      String testName = test.getKey();
      Properties testProps = new Properties();
      for (Map.Entry<String, Integer> prop: test.getValue().entrySet()) {
        testProps.setProperty(prop.getKey(), prop.getValue().toString());
      }
      NewestKSelectionPolicy policy = new NewestKSelectionPolicy(testProps);
      Assert.assertEquals(policy.getVersionsSelected(),
                          Math.abs(TEST_RESULTS.get(testName).intValue()),
                          "Failure for test " + testName);
      Assert.assertEquals(policy.isExcludeMode(), TEST_RESULTS.get(testName).intValue() < 0,
                          "Failure for test " + testName);
    }

    for(Map.Entry<String, Map<String, Integer>> test: NEG_TEST_CONFIGS.entrySet()) {
      String testName = test.getKey();
      Properties testProps = new Properties();
      for (Map.Entry<String, Integer> prop: test.getValue().entrySet()) {
        testProps.setProperty(prop.getKey(), prop.getValue().toString());
      }
      try {
        new NewestKSelectionPolicy(testProps);
        Assert.fail("Exception expected for test " + testName);
      }
      catch (RuntimeException e) {
        //OK
      }
    }
  }

  @Test
  public void testCreationConfig() {
    for(Map.Entry<String, Map<String, Integer>> test: TEST_CONFIGS.entrySet()) {
      String testName = test.getKey();
      Config conf = ConfigFactory.parseMap(test.getValue());
      NewestKSelectionPolicy policy = new NewestKSelectionPolicy(conf);
      Assert.assertEquals(policy.getVersionsSelected(),
                          Math.abs(TEST_RESULTS.get(testName).intValue()),
                          "Failure for test " + testName);
      Assert.assertEquals(policy.isExcludeMode(), TEST_RESULTS.get(testName).intValue() < 0,
                          "Failure for test " + testName);
    }

    for(Map.Entry<String, Map<String, Integer>> test: NEG_TEST_CONFIGS.entrySet()) {
      String testName = test.getKey();
      Config conf = ConfigFactory.parseMap(test.getValue());
      try {
        new NewestKSelectionPolicy(conf);
        Assert.fail("Exception expected for test " + testName);
      }
      catch (RuntimeException e) {
        // OK
      }
    }
  }

  @Test
  public void testSelect() {
    ArrayList<DatasetVersion> versions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      versions.add(new TestStringDatasetVersion(String.format("v%03d", i)));
    }

    //selectedVersions 5 < 10
    Config conf = ConfigFactory.empty()
          .withValue(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY,
                     ConfigValueFactory.fromAnyRef(5));
    NewestKSelectionPolicy policy = new NewestKSelectionPolicy(conf);
    Collection<DatasetVersion> res = policy.listSelectedVersions(versions);
    int idx = 0;
    Assert.assertEquals(res.size(), policy.getVersionsSelected());
    for (DatasetVersion v: res) {
      Assert.assertEquals(v, versions.get(idx++), "Mismatch for index " + idx);
    }

    //selectedVersions 15 > 10
    conf = ConfigFactory.empty()
          .withValue(NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY,
                     ConfigValueFactory.fromAnyRef(15));
    policy = new NewestKSelectionPolicy(conf);
    res = policy.listSelectedVersions(versions);
    idx = 0;
    Assert.assertEquals(res.size(), versions.size());
    for (DatasetVersion v: res) {
      Assert.assertEquals(v, versions.get(idx++), "Mismatch for index " + idx);
    }

    //notSelectedVersions 4 < 10
    conf = ConfigFactory.empty()
          .withValue(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY,
                     ConfigValueFactory.fromAnyRef(4));
    policy = new NewestKSelectionPolicy(conf);
    res = policy.listSelectedVersions(versions);
    idx = policy.getVersionsSelected();
    Assert.assertEquals(res.size(), versions.size() - policy.getVersionsSelected());
    for (DatasetVersion v: res) {
      Assert.assertEquals(v, versions.get(idx++), "Mismatch for index " + idx);
    }

    //notSelectedVersions 14 > 10
    conf = ConfigFactory.empty()
          .withValue(NewestKSelectionPolicy.NEWEST_K_VERSIONS_NOTSELECTED_KEY,
                     ConfigValueFactory.fromAnyRef(14));
    policy = new NewestKSelectionPolicy(conf);
    res = policy.listSelectedVersions(versions);
    Assert.assertEquals(res.size(), 0);
  }

}
