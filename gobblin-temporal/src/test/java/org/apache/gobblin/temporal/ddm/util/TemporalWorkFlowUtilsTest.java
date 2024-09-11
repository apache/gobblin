package org.apache.gobblin.temporal.ddm.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TemporalWorkFlowUtilsTest {

  private Properties jobProps = Mockito.mock(Properties.class);

  @Test
  public void testGenerateGaasSearchAttributes() {
    // Mocking the properties
    Mockito.when(jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY)).thenReturn("group");
    Mockito.when(jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)).thenReturn("name");
    Mockito.when(jobProps.getProperty(Help.USER_TO_PROXY_KEY)).thenReturn("userProxy");

    // Expected attributes
    Map<String, Object> expectedAttributes = new HashMap<>();
    expectedAttributes.put(Help.GAAS_FLOW_KEY, "group.name");
    expectedAttributes.put(Help.USER_TO_PROXY_SEARCH_KEY, "userProxy");

    // Actual attributes
    Map<String, Object> actualAttributes = TemporalWorkFlowUtils.generateGaasSearchAttributes(jobProps);

    // Assertions
    Assert.assertEquals(expectedAttributes.get(Help.GAAS_FLOW_KEY), actualAttributes.get(Help.GAAS_FLOW_KEY));
    Assert.assertEquals(expectedAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY), actualAttributes.get(Help.USER_TO_PROXY_SEARCH_KEY));

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

    Assert.assertNull(result);
  }
}
