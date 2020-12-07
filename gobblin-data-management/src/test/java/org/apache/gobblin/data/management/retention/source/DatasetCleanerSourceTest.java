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

package org.apache.gobblin.data.management.retention.source;

import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.runtime.retention.DatasetCleanerTaskFactory;
import org.apache.gobblin.source.workunit.WorkUnit;


public class DatasetCleanerSourceTest {
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final String VALUE1 = "value1";
  private static final String VALUE2 = "value2";
  private static final String BAD_VALUE = "bad_value";
  private static final String TASK_FACTORY_KEY = "org.apache.gobblin.runtime.taskFactoryClass";

  private String getSourcePrefixed(String string) {
    return DatasetCleanerSource.DATASET_CLEANER_SOURCE_PREFIX + "." + string;
  }

  private String getConfigPrefixed(String configName, String string) {
    return getSourcePrefixed(configName + "." + string);
  }

  @Test
  public void testSingleConfig() {
    DatasetCleanerSource source = new DatasetCleanerSource();
    SourceState sourceState = new SourceState();
    Properties props = new Properties();


    props.put(KEY1, VALUE1);
    props.put(KEY2, VALUE2);

    sourceState.setProps(props, new Properties());

    List<WorkUnit> workUnits = source.getWorkunits(sourceState);

    Assert.assertEquals(workUnits.size(), 1);
    Assert.assertEquals(workUnits.get(0).getProp(TASK_FACTORY_KEY), DatasetCleanerTaskFactory.class.getName());
    Assert.assertEquals(workUnits.get(0).getProp(KEY1), VALUE1);
    Assert.assertEquals(workUnits.get(0).getProp(KEY2), VALUE2);
  }

  @Test
  public void testMultipleConfig() {
    DatasetCleanerSource source = new DatasetCleanerSource();
    SourceState sourceState = new SourceState();
    Properties props = new Properties();

    props.put(DatasetCleanerSource.DATASET_CLEANER_CONFIGURATIONS, "config1, config2");

    // test that config scoped config overrides source and base config
    props.put(KEY1, BAD_VALUE);
    props.put(getSourcePrefixed(KEY1), BAD_VALUE);
    props.put(getConfigPrefixed("config1", KEY1), VALUE1);

    // Test that source scoped config overrides base config
    props.put(KEY2, BAD_VALUE);
    props.put(getSourcePrefixed(KEY2), VALUE2);

    sourceState.setProps(props, new Properties());

    List<WorkUnit> workUnits = source.getWorkunits(sourceState);

    Assert.assertEquals(workUnits.size(), 2);
    Assert.assertEquals(workUnits.get(0).getProp(TASK_FACTORY_KEY), DatasetCleanerTaskFactory.class.getName());
    Assert.assertEquals(workUnits.get(1).getProp(TASK_FACTORY_KEY), DatasetCleanerTaskFactory.class.getName());

    Assert.assertEquals(workUnits.get(0).getProp(KEY1), VALUE1);
    Assert.assertEquals(workUnits.get(0).getProp(KEY2), VALUE2);

    Assert.assertEquals(workUnits.get(1).getProp(KEY1), BAD_VALUE);
    Assert.assertEquals(workUnits.get(1).getProp(KEY2), VALUE2);
  }
}
