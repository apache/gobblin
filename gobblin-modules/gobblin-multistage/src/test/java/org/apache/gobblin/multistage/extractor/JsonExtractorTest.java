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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.JsonExtractorKeys;
import org.apache.gobblin.multistage.keys.SourceKeys;
import org.apache.gobblin.multistage.source.HttpSource;
import org.apache.gobblin.multistage.source.MultistageSource;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.gobblin.multistage.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@Test
public class JsonExtractorTest {

  // Matches to the total count field in the response json
  private static final int TOTAL_COUNT = 2741497;
  private final static String DATA_SET_URN_KEY = "SeriesCollection";
  private final static String ACTIVATION_PROP = "{\"name\": \"survey\", \"type\": \"unit\", \"units\": \"id1,id2\"}";
  private final static long WORKUNIT_STARTTIME_KEY = 1590994800000L;

  private Gson gson;
  private ExtractorKeys extractorKeys;
  private SourceKeys sourceKeys;
  private WorkUnit workUnit;
  private WorkUnitState state;
  private WorkUnitStatus workUnitStatus;
  private MultistageSource source;
  private JsonExtractorKeys jsonExtractorKeys;
  private JsonExtractor jsonExtractor;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    extractorKeys = Mockito.mock(ExtractorKeys.class);
    source = Mockito.mock(MultistageSource.class);
    sourceKeys = Mockito.mock(SourceKeys.class);
    workUnit = Mockito.mock(WorkUnit.class);
    workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    state = Mockito.mock(WorkUnitState.class);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORKUNIT_STARTTIME_KEY.getConfig(), 0L)).thenReturn(WORKUNIT_STARTTIME_KEY);
    when(state.getWorkunit()).thenReturn(workUnit);
    when(workUnit.getProp(ConfigurationKeys.DATASET_URN_KEY)).thenReturn(DATA_SET_URN_KEY);
    when(source.getSourceKeys()).thenReturn(sourceKeys);
    jsonExtractorKeys = Mockito.mock(JsonExtractorKeys.class);
    jsonExtractor = new JsonExtractor(state, source);
    jsonExtractor.jsonExtractorKeys = jsonExtractorKeys;
    jsonExtractor.extractorKeys = extractorKeys;
  }

  @Test
  public void testReadRecord() {
    when(sourceKeys.getTotalCountField()).thenReturn("totalRecords");
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(0));
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(null);
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(sourceKeys.getTotalCountField()).thenReturn(StringUtils.EMPTY);
    when(workUnitStatus.getMessages()).thenReturn(ImmutableMap.of("contentType", "application/json"));
    when(source.getNext(jsonExtractor)).thenReturn(workUnitStatus);
    InputStream stream = new ByteArrayInputStream("{\"key\":\"value\"}".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    when(sourceKeys.getDataField()).thenReturn(StringUtils.EMPTY);
    when(sourceKeys.getSessionKeyField()).thenReturn(new JsonObject());
    JsonSchema outputSchema = Mockito.mock(JsonSchema.class);
    when(sourceKeys.getOutputSchema()).thenReturn(outputSchema);
    when(outputSchema.getSchema()).thenReturn(gson.fromJson("{\"key\":\"value\"}", JsonObject.class));
    when(jsonExtractorKeys.getCurrentPageNumber()).thenReturn(Long.valueOf(0));
    when(extractorKeys.getSessionKeyValue()).thenReturn("session_key");
    when(workUnit.getProp(ConfigurationKeys.DATASET_URN_KEY)).thenReturn("com.abc.xxxxx.UserGroups");
    Iterator jsonElementIterator = ImmutableList.of().iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(0));
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(20));
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    JsonObject item = gson.fromJson("{\"key\":\"value\"}", JsonObject.class);
    jsonElementIterator = ImmutableList.of(item).iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(10));
    when(sourceKeys.getEncryptionField()).thenReturn(null);
    when(sourceKeys.isEnableCleansing()).thenReturn(true);
    Assert.assertEquals(jsonExtractor.readRecord(new JsonObject()).toString(), "{\"key\":\"value\"}");

    jsonElementIterator = ImmutableList.of().iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(10));
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(20));
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(10));
    when(sourceKeys.isPaginationEnabled()).thenReturn(true);
    when(sourceKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(sourceKeys.isPaginationEnabled()).thenReturn(false);
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(sourceKeys.isPaginationEnabled()).thenReturn(true);
    when(sourceKeys.isSessionStateEnabled()).thenReturn(true);
    when(sourceKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(extractorKeys.getSessionKeyValue()).thenReturn("success");
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));
  }

  @Test
  public void testProcessInputStream() {
    when(sourceKeys.getTotalCountField()).thenReturn(StringUtils.EMPTY);
    Assert.assertFalse(jsonExtractor.processInputStream(10));

    JsonElement item = new JsonObject();
    Iterator<JsonElement> jsonElementIterator = ImmutableList.of(item).iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(source.getNext(jsonExtractor)).thenReturn(null);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(workUnitStatus.getMessages()).thenReturn(ImmutableMap.of("contentType", "multipart/form-data"));
    when(source.getNext(jsonExtractor)).thenReturn(workUnitStatus);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(workUnitStatus.getMessages()).thenReturn(null);
    when(sourceKeys.hasSourceSchema()).thenReturn(true);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(sourceKeys.hasSourceSchema()).thenReturn(false);
    when(sourceKeys.hasOutputSchema()).thenReturn(true);
    Assert.assertFalse(jsonExtractor.processInputStream(0));
  }

  @Test
  public void testProcessInputStream2() {
    when(source.getNext(jsonExtractor)).thenReturn(workUnitStatus);
    when(workUnitStatus.getMessages()).thenReturn(null);
    when(workUnitStatus.getBuffer()).thenReturn(null);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    InputStream stream = new ByteArrayInputStream("null".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    stream = new ByteArrayInputStream("primitive_string".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    Assert.assertFalse(jsonExtractor.processInputStream(0));
  }

  @Test
  void testGetElementByJsonPath() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("");

    SourceState sourceState = mock(SourceState.class);
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(sourceState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new HttpSource();
    source.getWorkunits(sourceState);

    JsonArray jsonData = new Gson().fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/users.json")), JsonArray.class);
    String jsonPath = "settings.webConferencesRecorded";
    JsonExtractor extractor = new JsonExtractor(state, source);

    Assert.assertEquals(extractor.getElementByJsonPath(
        jsonData.get(0).getAsJsonObject(), jsonPath).getAsString(), "false");
  }

  /**
   * test GetElementByJsonPath when there is a JsonArray. In such case,
   * the JsonPath will have an Index, instead of a column name
   *
   * The sample Json response use "rows" as the data field name (ms.data.fields = rows).
   * And in each row, the first element "keys" is a JsonArray of 4 elements
   *
   * Input  1: JsonPath = keys.1
   * Output 1: abc
   *
   * Input  2: JsonPath = keys.3
   * Output 2: 2020-04-20
   *
   * Input  3: JsonPath = keys.4 (out of boundary)
   * Output 2: Blank
   */
  @Test
  void testGetElementByJsonPath2() {
    WorkUnitState state = new WorkUnitState();
    MultistageSource source = new HttpSource();
    JsonObject jsonData = new Gson().fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-row-with-array.json")), JsonObject.class);
    JsonExtractor extractor = new JsonExtractor(state, source);

    JsonObject row = jsonData.get("rows").getAsJsonArray().get(0).getAsJsonObject();
    Assert.assertEquals(extractor.getElementByJsonPath(row, "keys.1").getAsString(), "abc");
    Assert.assertEquals(extractor.getElementByJsonPath(row, "keys.3").getAsString(), "2020-04-20");

    String strValue = "";
    JsonElement ele = extractor.getElementByJsonPath(row, "keys.4");
    if (ele != null && !ele.isJsonNull()) {
      strValue = ele.getAsString();
    }
    Assert.assertEquals(strValue, "");
  }

  @Test
  public void testGetElementByJsonPathWithEdgeCases() {
    JsonObject row = new JsonObject();
    String jsonPath = StringUtils.EMPTY;
    Assert.assertEquals(jsonExtractor.getElementByJsonPath(row, jsonPath), JsonNull.INSTANCE);

    jsonPath = "key";
    Assert.assertEquals(jsonExtractor.getElementByJsonPath(null, jsonPath), JsonNull.INSTANCE);

    row = gson.fromJson("{\"key\":\"some_primitive_value\"}", JsonObject.class);
    jsonPath = "key.1";
    Assert.assertEquals(jsonExtractor.getElementByJsonPath(row, jsonPath), JsonNull.INSTANCE);

    row = gson.fromJson("{\"key\":[\"some_primitive_value\"]}", JsonObject.class);
    jsonPath = "key.a";
    Assert.assertEquals(jsonExtractor.getElementByJsonPath(row, jsonPath), JsonNull.INSTANCE);

    jsonPath = "key.3";
    Assert.assertEquals(jsonExtractor.getElementByJsonPath(row, jsonPath), JsonNull.INSTANCE);
  }

  /**
   * Test Extractor shall stop the session when total count of records is met
   */
  @Test
  void testStopConditionTotalCountMet() {
    WorkUnitState state = mock(WorkUnitState.class);
    InputStream inputStream = getClass().getResourceAsStream("/json/last-page-with-data.json");
    WorkUnitStatus status = WorkUnitStatus.builder()
        .messages(new HashMap<>())
        .sessionKey("")
        .buffer(inputStream).build();
    status.setTotalCount(TOTAL_COUNT);

    SourceState sourceState = mock(SourceState.class);
    when(sourceState.getProp("ms.data.field", "")).thenReturn("items");
    when(sourceState.getProp("ms.total.count.field", "")).thenReturn("totalResults");
    when(sourceState.getProp("ms.pagination", "")).thenReturn("{\"fields\": [\"offset\", \"limit\"], \"initialvalues\": [0, 5000]}");
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(sourceState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new HttpSource();
    source.getWorkunits(sourceState);

    MultistageSource mockSource = mock(HttpSource.class);
    JsonExtractor extractor = new JsonExtractor(state, mockSource);
    extractor.jsonExtractorKeys.setTotalCount(TOTAL_COUNT);

    when(mockSource.getFirst(extractor)).thenReturn(status);
    doNothing().when(mockSource).closeStream(extractor);
    when(mockSource.getSourceKeys()).thenReturn(source.getSourceKeys());

    Assert.assertFalse(extractor.processInputStream(TOTAL_COUNT));
    // If total count not reached, should not fail
    Assert.assertTrue(extractor.processInputStream(TOTAL_COUNT-1));
  }

  @Test
  void testCounts() {
    WorkUnitState state = mock(WorkUnitState.class);
    InputStream inputStream = getClass().getResourceAsStream("/util/users.json");
    WorkUnitStatus status = WorkUnitStatus.builder()
        .messages(new HashMap<>())
        .sessionKey("")
        .buffer(inputStream).build();

    SourceState sourceState = mock(SourceState.class);

    when(sourceState.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    HttpSource source = new HttpSource();
    source.getWorkunits(sourceState);

    HttpSource mockSource = mock(HttpSource.class);
    JsonExtractor extractor = new JsonExtractor(state, mockSource);

    when(mockSource.getFirst(extractor)).thenReturn(status);
    doNothing().when(mockSource).closeStream(extractor);
    when(mockSource.getSourceKeys()).thenReturn(source.getSourceKeys());

    extractor.getSchema();
    Assert.assertEquals(extractor.jsonExtractorKeys.getTotalCount(), 1);
    Assert.assertEquals(extractor.getWorkUnitStatus().getTotalCount(), 1);
    Assert.assertEquals(extractor.getWorkUnitStatus().getSetCount(), 1);
    Assert.assertEquals(extractor.getWorkUnitStatus().getPageStart(), 1);
  }

  @Test
  public void testAddDerivedFields() throws Exception {
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    jsonExtractor.setTimezone("America/Los_Angeles");
    JsonObject row = new JsonObject();
    JsonObject pushDowns = new JsonObject();
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(), "{}");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("start_time", "2020-06-01");
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    JsonObject actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"2020-06-01\"");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "string", "source", "P0D", "format", "yyyy-MM-dd"));
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(),
        "{\"start_time\":\"2020-06-01\",\"formula\":\"\"}");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "P0D", "format", "yyyy-MM-dd"));
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"2020-06-01\"");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("start_time", "1592809200000");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "regexp", "source", "uri", "format", "/syncs/([0-9]+)$"));
    when(sourceKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("uri", "invalid_uri");
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(),
        "{\"start_time\":\"1592809200000\",\"formula\":\"no match\",\"uri\":\"invalid_uri\"}");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 3);
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");
    Assert.assertEquals(actual.get("formula").toString(), "\"no match\"");
    Assert.assertEquals(actual.get("uri").toString(), "\"invalid_uri\"");
  }

  @Test
  public void testGetNextPaginationValues() throws Exception {
    Map<ParameterTypes, String> paginationKeys = ImmutableMap.of(
        ParameterTypes.PAGESTART, "page_start",
        ParameterTypes.PAGESIZE, "page_size",
        ParameterTypes.PAGENO, "page_number");

    when(sourceKeys.getPaginationFields()).thenReturn(paginationKeys);
    JsonElement input = gson.fromJson("{\"page_start\":0, \"page_size\":100, \"page_number\":1}", JsonObject.class);
    HashMap<ParameterTypes, Long>
        paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 3);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGESIZE), Long.valueOf(100));
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGESTART), Long.valueOf(100));

    input = gson.fromJson("{\"page_size\":100, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("{\"page_start\":0, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("{\"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    gson.fromJson("{\"page_start\":null, \"page_size\":100, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    gson.fromJson("{\"page_start\":0, \"page_size\":null, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("test_primitive_value", JsonPrimitive.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 0);
  }

  @Test
  public void testRetrieveSessionKeyValue() throws Exception {
    JsonObject sessionKeyField = gson.fromJson("{\"name\": \"hasMore\", \"condition\": {\"regexp\": \"false|False\"}}", JsonObject.class);
    when(sourceKeys.getSessionKeyField()).thenReturn(sessionKeyField);
    JsonElement input = gson.fromJson("[{\"name\": \"hasMore\"}]", JsonArray.class);
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrieveSessionKeyValue", input), StringUtils.EMPTY);

    input = gson.fromJson("{\"notMore\": \"someValue\"}", JsonObject.class);
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrieveSessionKeyValue", input), StringUtils.EMPTY);
  }

  @Test
  public void testGetTotalCountValue() throws Exception {
    JsonElement data = gson.fromJson(
        new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-data-with-total-count.json")), JsonObject.class);
    int currentCount = 10;
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(currentCount));
    when(source.getSourceKeys().getTotalCountField()).thenReturn("");
    when(source.getSourceKeys().getDataField()).thenReturn("items");
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(2 + currentCount));

    when(source.getSourceKeys().getDataField()).thenReturn("dataitems");
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(currentCount));

    data = gson.fromJson("[{\"callId\":\"001\"},{\"callId\":\"002\"},{\"callId\":\"003\"}]", JsonArray.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(3 + currentCount));

    data = gson.fromJson("callId", JsonPrimitive.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(currentCount));

    data = gson.fromJson("{\"items\":{\"callId\":\"001\"}}", JsonObject.class);
    when(source.getSourceKeys().getTotalCountField()).thenReturn("totalRecords");
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(currentCount));
  }

  /**
   * Test getTotalCountValue with non-JsonArray payload
   * Expect: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGetTotalCountValueWithJsonObjectPayload() throws Exception {
    when(source.getSourceKeys().getTotalCountField()).thenReturn("");
    when(source.getSourceKeys().getDataField()).thenReturn("items");
    JsonObject data = gson.fromJson("{\"records\":{\"totalRecords\":2},\"items\":{\"callId\":\"001\"}}", JsonObject.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(0));
  }

  @Test
  public void testLimitedCleanse() throws Exception {
    JsonElement input;
    input = gson.fromJson("{\"key\": \"value\"}", JsonObject.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());

    input = gson.fromJson("[{\"key\": \"value\"}]", JsonArray.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());

    input = gson.fromJson("test_primitive_value", JsonPrimitive.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());
  }

  /**
   * Test the timeout scenario: session timeout and condition is not met
   * @throws Exception
   */
  @Test(expectedExceptions = {RuntimeException.class})
  public void testWaitingBySessionKeyWithTimeout() throws Exception {
    when(sourceKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));

    when(sourceKeys.isSessionStateEnabled()).thenReturn(true);
    when(sourceKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(extractorKeys.getSessionKeyValue()).thenReturn("failed");
    long secondsBeforeCurrentTime = DateTime.now().minus(3000).getMillis();
    long timeout = 2000;
    when(extractorKeys.getStartTime()).thenReturn(secondsBeforeCurrentTime);
    when(source.getSourceKeys().getSessionTimeout()).thenReturn(timeout);
    Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout");
  }

  /**
   * Test the in-session scenario: session condition not met, but not timeout, therefore no exception
   * @throws Exception
   */
  @Test
  public void testWaitingBySessionKeyWithTimeout2() throws Exception {
    when(sourceKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));

    when(sourceKeys.isSessionStateEnabled()).thenReturn(true);
    when(sourceKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(extractorKeys.getSessionKeyValue()).thenReturn("failed");
    long secondsBeforeCurrentTime = DateTime.now().minus(3000).getMillis();
    long timeout = 4000;
    when(extractorKeys.getStartTime()).thenReturn(secondsBeforeCurrentTime);
    when(source.getSourceKeys().getSessionTimeout()).thenReturn(timeout);
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));
  }

  @Test
  public void testIsSessionStateMatch() throws Exception {
    when(sourceKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));

    when(sourceKeys.isSessionStateEnabled()).thenReturn(true);
    when(sourceKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(extractorKeys.getSessionKeyValue()).thenReturn("success");
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));

    when(extractorKeys.getSessionKeyValue()).thenReturn("failed");
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));
  }

  @Test
  public void testRetrievePushDowns() throws Exception {
    Map<String, Map<String, String>> derivedFields = new HashMap<>();
    JsonElement response = null;
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());

    response = JsonNull.INSTANCE;
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());

    response = new JsonArray();
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());
  }

  @Test
  public void testExtractJson() throws Exception {
    InputStream input = null;
    Assert.assertNull(Whitebox.invokeMethod(jsonExtractor, "extractJson", input));
  }
}
