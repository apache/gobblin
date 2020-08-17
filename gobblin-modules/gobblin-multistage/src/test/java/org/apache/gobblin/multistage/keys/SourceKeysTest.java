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

package org.apache.gobblin.multistage.keys;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gobblin.configuration.SourceState;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.source.MultistageSource;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.junit.Assert;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@PrepareForTest({JsonUtils.class})
public class SourceKeysTest extends PowerMockTestCase {
  private SourceKeys sourceKeys;
  private Gson gson;

  @BeforeMethod
  public void setUp() {
    sourceKeys = new SourceKeys();
    gson = new Gson();
  }

  @Test
  public void testIsSessionStateEnabled() {
    JsonObject sessions = new JsonObject();
    sourceKeys.setSessionKeyField(sessions);
    Assert.assertFalse(sourceKeys.isSessionStateEnabled());

    sessions.addProperty("non-condition", false);
    Assert.assertFalse(sourceKeys.isSessionStateEnabled());

    JsonObject nestedObj = new JsonObject();
    sessions.add("condition", nestedObj);
    Assert.assertFalse(sourceKeys.isSessionStateEnabled());
    Assert.assertEquals(sourceKeys.getSessionStateCondition(), StringUtils.EMPTY);

    nestedObj.addProperty("regexp", "testValue");
    sessions.add("condition", nestedObj);
    Assert.assertTrue(sourceKeys.isSessionStateEnabled());
    Assert.assertEquals(sourceKeys.getSessionStateCondition(), "testValue");
  }

  @Test
  public void testHasSourceSchema() {
    JsonSchema sourceSchema = Mockito.mock(JsonSchema.class);
    JsonSchema outSourceSchema = Mockito.mock(JsonSchema.class);
    JsonObject expected = new JsonObject();
    when(sourceSchema.getSchema()).thenReturn(expected);
    Assert.assertFalse(sourceKeys.hasSourceSchema());

    expected = new JsonObject();
    expected.addProperty("testKey", "testValue");

    JsonElement element = gson.fromJson("[]", JsonElement.class);
    JsonArray jsonArray = new JsonArray();
    when(sourceSchema.getAltSchema(sourceKeys.getDefaultFieldTypes(), sourceKeys.isEnableCleansing())).thenReturn(jsonArray);
    PowerMockito.mockStatic(JsonUtils.class);
    PowerMockito.when(JsonUtils.deepCopy(jsonArray)).thenReturn(element);
    when(outSourceSchema.getSchema()).thenReturn(new JsonObject());

    sourceKeys.setOutputSchema(outSourceSchema);
    sourceKeys.setSourceSchema(sourceSchema);

    when(sourceSchema.getSchema()).thenReturn(expected);
    Assert.assertTrue(sourceKeys.hasSourceSchema());
  }

  @Test
  public void testIsPaginationEnabled() {
    Assert.assertFalse(sourceKeys.isPaginationEnabled());

    Map<ParameterTypes, String> paginationFields = new HashMap<>();
    paginationFields.put(ParameterTypes.PAGESIZE, "testValue");
    sourceKeys.setPaginationFields(paginationFields);
    Assert.assertTrue(sourceKeys.isPaginationEnabled());

    paginationFields = new HashMap<>();
    sourceKeys.setPaginationFields(paginationFields);
    Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
    paginationInitValues.put(ParameterTypes.PAGESIZE, 100L);
    sourceKeys.setPaginationInitValues(paginationInitValues);
    Assert.assertTrue(sourceKeys.isPaginationEnabled());
  }

  /**
   * Test the validate() method
   *
   * Scenario 1: pagination defined, but no total count field, nor session key field
   *
   * Scenario 2: wrong output schema structure
   */
  @Test
  public void testValidation() {
    // test pagination parameter validation
    SourceState state = new SourceState();
    Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
    paginationInitValues.put(ParameterTypes.PAGESTART, 0L);
    paginationInitValues.put(ParameterTypes.PAGESIZE, 100L);
    sourceKeys.setPaginationInitValues(paginationInitValues);
    Assert.assertTrue(sourceKeys.validate(state));

    // test output schema validation with a wrong type
    state.setProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "{}");
    Assert.assertFalse(sourceKeys.validate(state));

    // test output schema validation with an empty array
    state.setProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "[{}]");
    Assert.assertFalse(sourceKeys.validate(state));

    // test output schema validation with an incorrect structure
    String schema = "[{\"columnName\":\"test\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}]";
    state.setProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), schema);
    sourceKeys.setOutputSchema(new MultistageSource<>().parseOutputSchema(state));
    Assert.assertFalse(sourceKeys.validate(state));

    schema = "[{\"columnName\":\"test\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]";
    state.setProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), schema);
    sourceKeys.setOutputSchema(new MultistageSource<>().parseOutputSchema(state));
    Assert.assertTrue(sourceKeys.validate(state));

    state.setProp(MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getConfig(), "lovely");
    sourceKeys.setWorkUnitPartitionType(null);
    Assert.assertFalse(sourceKeys.validate(state));

    state.setProp(MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getConfig(), "{\"weekly\": [\"2020-01-01\", \"2020-02-1\"]}");
    sourceKeys.setWorkUnitPartitionType(WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertFalse(sourceKeys.validate(state));
  }
}