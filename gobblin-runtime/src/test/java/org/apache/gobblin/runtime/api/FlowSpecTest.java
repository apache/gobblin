package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.FlowId;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FlowSpecTest {

  /**
   * Tests that the addProperty() function to ensure the new flowSpec returned has the original properties and updated
   * ones
   * @throws URISyntaxException
   */
  @Test
  public void testAddProperty() throws URISyntaxException {
    String flowGroup = "myGroup";
    String flowName = "myName";
    String flowExecutionId = "myId";
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);

    // Create properties to be used as config
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    properties.setProperty(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    properties.setProperty(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY, "true");

    FlowSpec originalFlowSpec = FlowSpec.builder(flowUri).withConfigAsProperties(properties).build();
    FlowSpec updatedFlowSpec = originalFlowSpec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);

    Properties updatedProperties = updatedFlowSpec.getConfigAsProperties();
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_GROUP_KEY), flowGroup);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_NAME_KEY), flowName);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY), "true");
  }
}
