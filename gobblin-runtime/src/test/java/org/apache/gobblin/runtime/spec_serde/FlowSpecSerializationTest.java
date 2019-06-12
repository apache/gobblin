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

package org.apache.gobblin.runtime.spec_serde;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.runtime.api.FlowSpec;


public class FlowSpecSerializationTest {
  private Gson gson = new GsonBuilder().registerTypeAdapter(new TypeToken<FlowSpec>() {}.getType(), new FlowSpecSerializer())
      .registerTypeAdapter(new TypeToken<FlowSpec>() {}.getType(), new FlowSpecDeserializer()).create();

  private FlowSpec flowSpec1;
  private FlowSpec flowSpec2;
  private FlowSpec flowSpec3;

  @BeforeClass
  public void setUp() throws URISyntaxException {
    gson = new GsonBuilder().registerTypeAdapter(new TypeToken<FlowSpec>() {}.getType(), new FlowSpecSerializer())
        .registerTypeAdapter(new TypeToken<FlowSpec>() {}.getType(), new FlowSpecDeserializer()).create();

    flowSpec1 = FlowSpec.builder("flowspec1").withVersion("version1").withDescription("description1")
        .withConfig(ConfigBuilder.create().addPrimitive("key1", "value1").build()).build();
    flowSpec2 = FlowSpec.builder("flowspec2").withVersion("version2").withDescription("description2")
        .withConfig(ConfigBuilder.create().addPrimitive("key2", "value2").build()).build();
    flowSpec3 = FlowSpec.builder("flowspec3").withVersion("version3").withDescription("description3")
        .withConfig(ConfigBuilder.create().addPrimitive("key3", "value3").build())
        .withTemplates(Arrays.asList(new URI("template1"), new URI("template2")))
        .withChildSpecs(Arrays.asList(flowSpec1, flowSpec2)).build();
  }

  @Test
  public void testSerializeWithNoChildren() {
    String json = gson.toJson(flowSpec1);
    Assert.assertEquals(gson.fromJson(json, FlowSpec.class), flowSpec1);
  }

  @Test
  public void testSerializeWithChildren() {
    String json = gson.toJson(flowSpec3);
    Assert.assertEquals(gson.fromJson(json, FlowSpec.class), flowSpec3);
  }
}
