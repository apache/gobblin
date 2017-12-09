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
package org.apache.gobblin.runtime.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import org.apache.gobblin.configuration.ConfigurationKeys;


@Test(groups = { "gobblin.runtime" })
public class JobStateToJsonConverterTest {

  private final String PROPERTIES = "properties";
  private final String TASK_STATES = "task states";
  private final String TEST_JOB = "TestJob";
  private final String TEST_STORE = "store/";

//  Disable test for now. It will be re-enabled after we have a current.jst with new class name states
  @Test
  public void testJsonKeepConfig()
      throws IOException {
    String stateStorePath = getClass().getClassLoader().getResource(TEST_STORE).getPath();

    boolean keepConfig = true;

    JobStateToJsonConverter converter = new JobStateToJsonConverter(new Properties(), stateStorePath, keepConfig);

    StringWriter stringWriter = new StringWriter();
    converter.convert(TEST_JOB, stringWriter);

    JsonObject json = new JsonParser().parse(new JsonReader(new StringReader(stringWriter.toString()))).getAsJsonObject();

    Assert.assertNotNull(json.get(PROPERTIES));
    for (JsonElement taskState: json.get(TASK_STATES).getAsJsonArray()) {
      Assert.assertNotNull(taskState.getAsJsonObject().get(PROPERTIES));
    }
  }

  @Test
  public void testJsonKeepConfigWithoutStoreUrl()
      throws IOException {
    String stateStorePath = getClass().getClassLoader().getResource(TEST_STORE).getPath();
    Properties properties = new Properties();

    properties.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStorePath);

    boolean keepConfig = true;

    JobStateToJsonConverter converter = new JobStateToJsonConverter(properties, null, keepConfig);

    StringWriter stringWriter = new StringWriter();
    converter.convert(TEST_JOB, stringWriter);

    JsonObject json = new JsonParser().parse(new JsonReader(new StringReader(stringWriter.toString()))).getAsJsonObject();

    Assert.assertNotNull(json.get(PROPERTIES));
    for (JsonElement taskState: json.get(TASK_STATES).getAsJsonArray()) {
      Assert.assertNotNull(taskState.getAsJsonObject().get(PROPERTIES));
    }
  }
}
