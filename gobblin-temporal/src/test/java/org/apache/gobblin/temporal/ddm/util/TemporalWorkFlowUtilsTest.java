package org.apache.gobblin.temporal.ddm.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;



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
    expectedAttributes.put(Help.GAAS_FLOW_ID, "group.name");
    expectedAttributes.put(Help.USER_TO_PROXY_KEY, "userProxy");

    // Actual attributes
    Map<String, Object> actualAttributes = TemporalWorkFlowUtils.generateGaasSearchAttributes(jobProps);

    // Assertions
    Assert.assertEquals(expectedAttributes, actualAttributes);
  }

  @Test
  public void testGenerateGaasSearchAttributesWithNullProps() {
    // Expecting a NullPointerException when jobProps is null
    Assert.assertThrows(NullPointerException.class, () -> {
      TemporalWorkFlowUtils.generateGaasSearchAttributes(null);
    });
  }
}
