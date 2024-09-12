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
package org.apache.gobblin.temporal.ddm.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TemporalWorkFlowUtilsTest {

  @Test
  public void testGenerateGaasSearchAttributes() {
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, "group");
    jobProps.setProperty(ConfigurationKeys.FLOW_NAME_KEY, "name");
    jobProps.setProperty(Help.USER_TO_PROXY_KEY, "userProxy");

    // Expected attributes
    Map<String, Object> expectedAttributes = new HashMap<>();
    expectedAttributes.put(Help.GAAS_FLOW_ID_SEARCH_KEY, "group.name");
    expectedAttributes.put(Help.USER_TO_PROXY_SEARCH_KEY, "userProxy");

    // Actual attributes
    Map<String, Object> actualAttributes = TemporalWorkFlowUtils.generateGaasSearchAttributes(jobProps);

    // Assertions
    Assert.assertEquals(expectedAttributes.get(Help.GAAS_FLOW_ID_SEARCH_KEY), actualAttributes.get(Help.GAAS_FLOW_ID_SEARCH_KEY));
    Assert.assertEquals(expectedAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY),
        actualAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY));
  }

  @Test
  public void testGenerateGaasSearchAttributesWithMissingFlowGroup() {
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.FLOW_NAME_KEY, "name");
    jobProps.setProperty(Help.USER_TO_PROXY_KEY, "userProxy");

    // Expected attributes
    Map<String, Object> expectedAttributes = new HashMap<>();
    expectedAttributes.put(Help.GAAS_FLOW_ID_SEARCH_KEY, TemporalWorkFlowUtils.DEFAULT_GAAS_FLOW_GROUP + "." + "name");
    expectedAttributes.put(Help.USER_TO_PROXY_SEARCH_KEY, "userProxy");

    // Actual attributes
    Map<String, Object> actualAttributes = TemporalWorkFlowUtils.generateGaasSearchAttributes(jobProps);

    // Assertions
    Assert.assertEquals(expectedAttributes.get(Help.GAAS_FLOW_ID_SEARCH_KEY), actualAttributes.get(Help.GAAS_FLOW_ID_SEARCH_KEY));
    Assert.assertEquals(expectedAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY),
        actualAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY));
  }

  @Test
  public void testGenerateGaasSearchAttributesWithNullProps() {
    // Expecting a NullPointerException when jobProps is null
    Assert.assertThrows(NullPointerException.class, () -> {
      TemporalWorkFlowUtils.generateGaasSearchAttributes(null);
    });
  }

  @Test
  public void testConvertSearchAttributesValuesFromListToObject_withValidInput() {
    Map<String, List<?>> searchAttributes = new HashMap<>();
    searchAttributes.put("key1", Arrays.asList("value1", "value2"));
    searchAttributes.put("key2", Arrays.asList(1, 2, 3));

    Map<String, Object> result = TemporalWorkFlowUtils.convertSearchAttributesValuesFromListToObject(searchAttributes);

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get("key1"), Arrays.asList("value1", "value2"));
    Assert.assertEquals(result.get("key2"), Arrays.asList(1, 2, 3));
  }

  @Test
  public void testConvertSearchAttributesValuesFromListToObject_withEmptyInput() {
    Map<String, List<?>> searchAttributes = new HashMap<>();

    Map<String, Object> result = TemporalWorkFlowUtils.convertSearchAttributesValuesFromListToObject(searchAttributes);

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testConvertSearchAttributesValuesFromListToObject_withNullInput() {
    Map<String, List<?>> searchAttributes = null;

    Map<String, Object> result = TemporalWorkFlowUtils.convertSearchAttributesValuesFromListToObject(searchAttributes);

    Assert.assertTrue(result.isEmpty());
  }
}
