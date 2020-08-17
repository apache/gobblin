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

package org.apache.gobblin.multistage.extractor;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.filter.JsonSchemaBasedFilter;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.SourceKeys;
import org.apache.gobblin.multistage.source.MultistageSource;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@PrepareForTest({Thread.class, IOUtils.class})
public class MultistageExtractorTest extends PowerMockTestCase {
  private Gson gson;
  private ExtractorKeys extractorKeys;
  private MultistageExtractor multistageExtractor;
  private MultistageSource source;
  private WorkUnitState state;
  private WorkUnitStatus workUnitStatus;
  private SourceKeys sourceKeys;
  private JsonSchema jsonSchema;
  private JsonSchema outputSchema;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    extractorKeys = Mockito.mock(ExtractorKeys.class);
    state = mock(WorkUnitState.class);
    workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    source = mock(MultistageSource.class);
    sourceKeys = Mockito.mock(SourceKeys.class);
    jsonSchema = Mockito.mock(JsonSchema.class);
    outputSchema = Mockito.mock(JsonSchema.class);
    multistageExtractor = new MultistageExtractor(state, source);
  }

  @Test
  public void testJobProperties() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"activityDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"fromDateTime\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss'Z'\"}}]");
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");

    SourceState sourceState = mock(SourceState.class);

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{\"a\":\"x\"}");
    Assert.assertNotNull(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getProp(state));
    Assert.assertNotNull(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
    Assert.assertTrue(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertTrue(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{\"a\"}");
    Assert.assertFalse(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{}");
    Assert.assertTrue(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("");
    Assert.assertTrue(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
  }


  @Test
  public void testWorkUnitWatermark(){
    SourceState state = mock(SourceState.class);
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new MultistageSource();
    List<WorkUnit> workUnits = source.getWorkunits(state);
    WorkUnitState workUnitState = new WorkUnitState(workUnits.get(0));
    JsonExtractor extractor = new JsonExtractor(workUnitState, source);

    // low watermark by default is 2017-01-01
    Assert.assertEquals("1546329600000", extractor.getWorkUnitWaterMarks().get("low").getAsString());
  }

  @Test
  public void testGetSchema() {
    Assert.assertNull(multistageExtractor.getSchema());
  }

  @Test
  public void testGetExpectedRecordCount() {
    Assert.assertEquals(multistageExtractor.getExpectedRecordCount(), 0);
  }

  @Test
  public void testGetHighWatermark() {
    Assert.assertEquals(multistageExtractor.getHighWatermark(), 0);
  }

  @Test
  public void testReadRecord() {
    Assert.assertNull(multistageExtractor.readRecord(null));
  }

  @Test
  public void testClose() {
    when(state.getWorkingState()).thenReturn(WorkUnitState.WorkingState.CANCELLED);
    doNothing().when(source).closeStream(multistageExtractor);
    multistageExtractor.close();
  }

  @Test
  public void testProcessInputStream() {
    Assert.assertFalse(multistageExtractor.processInputStream(100L));
  }

  @Test
  public void testSetRowFilter() {
    JsonSchemaBasedFilter filter = Mockito.mock(JsonSchemaBasedFilter.class);
    JsonArray schema = new JsonArray();
    multistageExtractor.rowFilter = filter;
    multistageExtractor.setRowFilter(schema);

    multistageExtractor.rowFilter = null;
    when(state.getProp(MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    multistageExtractor.setRowFilter(new JsonArray());
    Assert.assertNull(multistageExtractor.rowFilter);
  }

  @Test
  public void testGetOrInferSchema() {
    JsonObject schema = new JsonObject();
    schema.addProperty("testAttribute", "something");

    JsonArray schemaArray = new JsonArray();
    Map<String, String> defaultFieldTypes = new HashMap<>();

    when(source.getSourceKeys()).thenReturn(sourceKeys);
    when(sourceKeys.getDefaultFieldTypes()).thenReturn(defaultFieldTypes);
    when(sourceKeys.isEnableCleansing()).thenReturn(true);
    when(sourceKeys.getSourceSchema()).thenReturn(jsonSchema);
    when(jsonSchema.getAltSchema(defaultFieldTypes, true)).thenReturn(schemaArray);

    when(sourceKeys.getOutputSchema()).thenReturn(outputSchema);
    when(outputSchema.getSchema()).thenReturn(schema);
    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);

    when(sourceKeys.hasOutputSchema()).thenReturn(false);
    when(sourceKeys.hasSourceSchema()).thenReturn(true);
    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);

    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    JsonSchema inferredSchema = Mockito.mock(JsonSchema.class);
    JsonObject schemaObj = new JsonObject();
    schemaObj.addProperty("type", "null");
    multistageExtractor.extractorKeys = extractorKeys;
    when(extractorKeys.getInferredSchema()).thenReturn(inferredSchema);
    when(inferredSchema.getSchema()).thenReturn(schemaObj);
    when(sourceKeys.hasSourceSchema()).thenReturn(false);
    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);
  }

  @Test
  public void testHoldExecutionUnitPresetStartTime() throws Exception {
    multistageExtractor.extractorKeys = extractorKeys;
    //current time + 3 s
    Long currentSeconds = DateTime.now().plusSeconds(3).getMillis();
    when(extractorKeys.getDelayStartTime()).thenReturn(currentSeconds);

    PowerMockito.mockStatic(Thread.class);
    PowerMockito.doNothing().when(Thread.class);
    Thread.sleep(100L);
    multistageExtractor.holdExecutionUnitPresetStartTime();

    when(extractorKeys.getDelayStartTime()).thenReturn(DateTime.now().plusSeconds(3).getMillis());
    PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
    Thread.sleep(100L);
    multistageExtractor.holdExecutionUnitPresetStartTime();
  }

  @Test
  public void testsFailWorkUnit() {
    state = new WorkUnitState();
    WorkUnitState stateSpy = spy(state);
    multistageExtractor.state = stateSpy;
    multistageExtractor.failWorkUnit(StringUtils.EMPTY);
    verify(stateSpy).setWorkingState(WorkUnitState.WorkingState.FAILED);
    multistageExtractor.failWorkUnit("NON_EMPTY_ERROR_STRING");
  }

  @Test
  public void testDeriveEpoc() {
    String format = "yyyy-MM-dd";
    String strValue = "2020-06-20";
    Assert.assertNotEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);

    strValue = "2018-07-14Txsdfs";
    Assert.assertNotEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);

    format = "yyyy-MM-dd'T'HH:mm:ssZ";
    strValue = "2018/07/14T14:31:30+0530";
    Assert.assertEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);
  }

  @Test
  public void testsAddDerivedFieldsToAltSchema() {
    Map<String, String> items = ImmutableMap.of("type", "some_type", "source", "token.full_token");
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula", items);
    multistageExtractor.source = source;
    JsonObject obj = gson.fromJson("{\"token.full_token\": {\"type\":\"string\"}}", JsonObject.class);
    when(source.getSourceKeys()).thenReturn(sourceKeys);
    when(sourceKeys.getOutputSchema()).thenReturn(outputSchema);
    when(outputSchema.get("properties")).thenReturn(obj);
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    Assert.assertEquals(multistageExtractor.addDerivedFieldsToAltSchema().toString(),
        "[{\"columnName\":\"formula\",\"dataType\":{\"type\":\"string\"}}]");
  }

  @Test
  public void testExtractText() throws Exception {
    Assert.assertEquals(multistageExtractor.extractText(null), StringUtils.EMPTY);

    String expected = "test_string";
    InputStream input = new ByteArrayInputStream(expected.getBytes());
    when(state.getProp(MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET.getConfig(), StringUtils.EMPTY)).thenReturn("UTF-8");
    doNothing().when(source).closeStream(multistageExtractor);
    multistageExtractor.source = source;
    Assert.assertEquals(multistageExtractor.extractText(input), expected);

    PowerMockito.mockStatic(IOUtils.class);
    PowerMockito.doThrow(new IOException()).when(IOUtils.class, "toString", input, Charset.forName("UTF-8"));
    multistageExtractor.extractText(input);
    Assert.assertEquals(multistageExtractor.extractText(null), StringUtils.EMPTY);
  }

  @Test
  public void testCheckContentType() {
    String expectedContentType = "application/json";
    Map<String, String> messages = new HashMap<>();
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    messages.put("contentType", expectedContentType);
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    messages.put("contentType", "non-expected-contentType");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertFalse(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    when(workUnitStatus.getMessages()).thenReturn(null);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));
  }

  /**
   * test getting session key value when the value is in the headers
   */
  @Test
  public void testGetSessionKeyValue() {
    String headers = "{\"cursor\": \"123\"}";
    Map<String, String> messages = new HashMap<>();
    messages.put("headers", headers);
    when(workUnitStatus.getMessages()).thenReturn(messages);

    JsonObject sessionKeyField = gson.fromJson("{\"name\": \"cursor\"}", JsonObject.class);
    when(source.getSourceKeys()).thenReturn(sourceKeys);
    when(sourceKeys.getSessionKeyField()).thenReturn(sessionKeyField);

    Assert.assertEquals(multistageExtractor.getSessionKey(workUnitStatus), "123");
  }
}