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

package org.apache.gobblin.multistage.util;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class WorkUnitStatusTest {

  /**
   * testing the builder function
   */
  @Test
  public void testDataMethods() {
    String expected = "WorkUnitStatus(totalCount=10, setCount=0, pageNumber=1, pageStart=0, pageSize=100, buffer=null, messages={name=text}, sessionKey=)";
    Map<String, String> messages = new HashMap<>();
    messages.put("name", "text");
    Assert.assertEquals(expected, WorkUnitStatus.builder()
        .pageNumber(1)
        .pageSize(100)
        .totalCount(10)
        .sessionKey("")
        .messages(messages)
        .build()
        .toString());
  }

  /**
   * test getting schema
   * scenario 1: default value
   * scenario 2: source provided value
   * scenario 3: source provided invalid value
   */
  public void testGetSchema() {
    // when there is no source provided schema, the getSchema() method
    // should just return a new JsonSchema object
    Assert.assertEquals(new JsonSchema(), WorkUnitStatus.builder()
        .build().getSchema());

    String expected = "{\"id\":{\"type\":\"string\"}}";
    Map<String, String> messages = new HashMap<>();
    messages.put("schema", "{\"id\": {\"type\": \"string\"}}");
    Assert.assertEquals(expected, WorkUnitStatus.builder()
        .messages(messages)
        .build().getSchema().toString());

    // source schema is invalid
    WorkUnitStatus.builder().messages(ImmutableMap.of("schema", "{\"id\": {\"type\": \"string\"}")).build().getSchema();

    // source schema is null
    Assert.assertEquals(WorkUnitStatus.builder().messages(null).build().getSchema(), new JsonSchema());
  }
}
