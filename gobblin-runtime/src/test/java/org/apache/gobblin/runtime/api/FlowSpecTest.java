package org.apache.gobblin.runtime.api;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gobblin.config.ConfigBuilder;
import org.junit.Assert;
import org.testng.annotations.Test;


public class FlowSpecTest {

  @Test
  public void testDuplicateKeysShareMem() throws URISyntaxException {
    FlowSpec flowSpec1 = FlowSpec.builder("flowspec1").withVersion("version1").withDescription("description1")
        .withConfig(ConfigBuilder.create().addPrimitive("key1", "value1").build()).build();
    FlowSpec flowSpec2 = FlowSpec.builder("flowspec2").withVersion("version2").withDescription("description2")
        .withConfig(ConfigBuilder.create().addPrimitive("key1", "value2").build()).build();

    List<String> flowSpec1Keys = flowSpec1.getConfigAsProperties().stringPropertyNames().stream().sorted().collect(
        Collectors.toList());
    List<String> flowSpec2Keys = flowSpec2.getConfigAsProperties().stringPropertyNames().stream().sorted().collect(
        Collectors.toList());;

    for (int i = 0; i < flowSpec1Keys.size(); i++) {
      // Since the keys are interned, they should resolve to the same object
      Assert.assertTrue(flowSpec1Keys.get(i) == flowSpec2Keys.get(i));
    }


  }

}
