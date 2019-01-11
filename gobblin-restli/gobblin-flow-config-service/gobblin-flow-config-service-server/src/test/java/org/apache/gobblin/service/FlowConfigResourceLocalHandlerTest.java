package org.apache.gobblin.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;

import org.apache.gobblin.runtime.api.FlowSpec;


public class FlowConfigResourceLocalHandlerTest {
  private static final String FLOW_GROUP_KEY = "flow.group";
  private static final String FLOW_NAME_KEY = "flow.name";
  private static final String SCHEDULE_KEY = "job.schedule";
  private static final String RUN_IMMEDIATELY_KEY = "flow.runImmediately";

  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  @Test
  public void testCreateFlowSpecForConfig() throws URISyntaxException {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "a:b:c*.d");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    FlowSpec flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);
    Assert.assertEquals(flowSpec.getConfig().getString(FLOW_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(SCHEDULE_KEY), TEST_SCHEDULE);
    Assert.assertEquals(flowSpec.getConfig().getBoolean(RUN_IMMEDIATELY_KEY), true);
    Assert.assertEquals(flowSpec.getConfig().getString("param1"), "a:b:c*.d");
    Assert.assertEquals(flowSpec.getTemplateURIs().get().size(), 1);
    Assert.assertTrue(flowSpec.getTemplateURIs().get().contains(new URI(TEST_TEMPLATE_URI)));

    flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put("param2", "${param1}-123");
    flowProperties.put("param3", "\"a:b:c*.d\"");
    flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));
    flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);

    Assert.assertEquals(flowSpec.getConfig().getString(FLOW_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(SCHEDULE_KEY), TEST_SCHEDULE);
    Assert.assertEquals(flowSpec.getConfig().getBoolean(RUN_IMMEDIATELY_KEY), true);
    Assert.assertEquals(flowSpec.getConfig().getString("param1"),"value1");
    Assert.assertEquals(flowSpec.getConfig().getString("param2"),"value1-123");
    Assert.assertEquals(flowSpec.getConfig().getString("param3"), "a:b:c*.d");
    Assert.assertEquals(flowSpec.getTemplateURIs().get().size(), 1);
    Assert.assertTrue(flowSpec.getTemplateURIs().get().contains(new URI(TEST_TEMPLATE_URI)));
  }
}