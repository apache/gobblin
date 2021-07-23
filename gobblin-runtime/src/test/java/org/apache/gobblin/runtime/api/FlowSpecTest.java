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
