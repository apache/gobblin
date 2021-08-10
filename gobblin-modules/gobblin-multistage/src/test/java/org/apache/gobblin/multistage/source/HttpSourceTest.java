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

package org.apache.gobblin.multistage.source;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.extractor.JsonExtractor;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.HttpSourceKeys;
import org.apache.gobblin.multistage.keys.SourceKeys;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.multistage.util.HttpRequestMethod;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.AutoRetryHttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.gobblin.multistage.configuration.MultistageProperties.*;
import static org.apache.gobblin.multistage.source.HttpSource.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


@Slf4j
@PrepareForTest({EncryptionUtils.class})
public class HttpSourceTest extends PowerMockTestCase {

  private Gson gson;
  private WorkUnitState state;
  private HttpSource source;
  private SourceKeys sourceKeys;
  private SourceState sourceState;
  private String token;
  private JsonObject pagination;
  private JsonObject sessionKeyField;
  private String totalCountField;
  private JsonArray parameters;
  private JsonArray encryptionFields;
  private String dataField;
  private Long callInterval;
  private Long waitTimeoutSeconds;
  private Boolean enableCleansing;
  private Boolean workUnitPartialPartition;
  private JsonArray watermark;
  private JsonArray secondaryInput;
  private String httpClientFactory;
  private JsonObject httpRequestHeaders;
  private String sourceUri;
  private String httpRequestMethod;
  private String extractorClass;
  private JsonObject authentication;
  private JsonObject httpStatus;
  private JsonObject httpStatusReasons;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    state = Mockito.mock(WorkUnitState.class);
    sourceKeys = Mockito.mock(SourceKeys.class);
    sourceState = Mockito.mock(SourceState.class);
    source = new HttpSource();
  }

  @Test(enabled = false)
  public void testAuthentication() {
    HttpSource source = new HttpSource();

    SourceState state = mock(SourceState.class);
    when(state.getProp("ms.watermark", "")).thenReturn("[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    when(state.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp("extract.namespace", "")).thenReturn("test");
    when(state.getProp("extract.table.name", "")).thenReturn("table1");
    when(state.getProp("source.conn.username", "")).thenReturn("X7CWBD5V4T6DR77WY23YSHACH55K2OXA");
    when(state.getProp("source.conn.password", "")).thenReturn("");
    when(state.getProp("ms.source.uri", "")).thenReturn("https://api.abc.io/v2/users");
    when(state.getProp("ms.authentication", new JsonObject().toString())).thenReturn("{\"method\":\"basic\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    when(state.getProp("ms.http.request.headers", new JsonObject().toString())).thenReturn("{\"Content-Type\": \"application/json\"}");
    when(state.getProp("ms.http.request.method", "")).thenReturn("GET");
    when(state.getProp("ms.session.key.field", new JsonObject().toString())).thenReturn("{\"name\": \"records.cursor\"}");
    when(state.getProp("ms.parameters", new JsonArray().toString())).thenReturn("[{\"name\":\"cursor\",\"type\":\"session\"}]");
    when(state.getProp("ms.data.field", "")).thenReturn("users");
    when(state.getProp("ms.total.count.field", "")).thenReturn("records.totalRecords");
    when(state.getProp("ms.work.unit.partition", "")).thenReturn("");
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{}");

    List<WorkUnit> workUnits = source.getWorkunits(state);

    Assert.assertFalse(source.getSourceKeys().isPaginationEnabled());
    Assert.assertNotNull(source.getSourceKeys());
    Assert.assertNotNull(source.getHttpSourceKeys());
    Assert.assertNotNull(source.getSourceKeys().getSourceParameters());
    Assert.assertTrue(workUnits.size() == 1);
    Assert.assertEquals(source.getHttpSourceKeys().getHttpRequestHeaders().toString(), "{\"Content-Type\":\"application/json\"}");

    WorkUnitState unitState = new WorkUnitState(workUnits.get(0));

    JsonExtractor extractor = new JsonExtractor(unitState, source);

    JsonObject record = extractor.readRecord(new JsonObject());

    // should return 14 columns
    Assert.assertEquals(14, record.entrySet().size());
    Assert.assertTrue(extractor.getWorkUnitStatus().getTotalCount() > 0);
    Assert.assertTrue(extractor.getWorkUnitStatus().getSessionKey().length() > 0);
  }

  /*
   * basic test with no watermark created.
   */
  @Test(enabled=false)
  public void getWorkUnitsTestEmpty() {
    HttpSource source = new HttpSource();
    List<WorkUnit> workUnits = source.getWorkunits(GobblinMultiStageTestHelpers.prepareSourceStateWithoutWaterMark());
    Assert.assertTrue(workUnits.size() == 1);
    Assert.assertEquals(workUnits.get(0).getLowWatermark().getAsJsonObject().get("value").toString(), "-1");
    Assert.assertEquals(workUnits.get(0).getExpectedHighWatermark().getAsJsonObject().get("value").toString(), "-1");
  }

  @Test(enabled=true)
  public void retriesTest() throws IOException {

    HttpSource source = new HttpSource();
    HttpClient mockHttpClient = mock(CloseableHttpClient.class);
    source.httpClient = mockHttpClient;
    HttpResponse httpResponse = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    HttpEntity entity = mock(HttpEntity.class);
    SourceState state = mock(SourceState.class);

    when(entity.getContent()).thenReturn(null);
    when(httpResponse.getEntity()).thenReturn(entity);
    when(statusLine.getStatusCode()).thenReturn(401);
    when(statusLine.getReasonPhrase()).thenReturn("pagination error");
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(source.httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    when(state.getProp("ms.watermark", "")).thenReturn("[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    when(state.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp("extract.namespace", "")).thenReturn("test");
    when(state.getProp("extract.table.name", "")).thenReturn("table1");
    when(state.getProp("source.conn.username", "")).thenReturn("X7CWBD5V4T6DR77WY23YSHACH55K2OXA");
    when(state.getProp("source.conn.password", "")).thenReturn("");
    when(state.getProp("ms.source.uri", "")).thenReturn("https://api.abc.io/v2/users");
    when(state.getProp("ms.authentication", new JsonObject().toString())).thenReturn("{\"method\":\"basic\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    when(state.getProp("ms.http.request.headers", new JsonObject().toString())).thenReturn("{\"Content-Type\": \"application/json\"}");
    when(state.getProp("ms.http.request.method", "")).thenReturn("GET");
    when(state.getProp("ms.session.key.field", new JsonObject().toString())).thenReturn("{\"name\": \"records.cursor\"}");
    when(state.getProp("ms.parameters", new JsonArray().toString())).thenReturn("[{\"name\":\"cursor\",\"type\":\"session\"}]");
    when(state.getProp("ms.data.field", "")).thenReturn("users");
    when(state.getProp("ms.total.count.field", "")).thenReturn("records.totalRecords");
    when(state.getProp("ms.work.unit.partition", "")).thenReturn("");
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{}");
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    List<WorkUnit> workUnits = source.getWorkunits(state);
    WorkUnitState unitState = new WorkUnitState(workUnits.get(0));

    JsonExtractor extractor = new JsonExtractor(unitState, source);

    JsonObject record = extractor.readRecord(new JsonObject());
    // since we are setting the buffer to null, the final record object will be null
    Assert.assertEquals(null, record);
  }

  /*
   * basic test with watermark.
   */
  @Test(enabled=false)
  public void getWorkUnitsTest() {
    HttpSource source = new HttpSource();
    List<WorkUnit> workUnits = source.getWorkunits(GobblinMultiStageTestHelpers.prepareSourceStateWithWaterMark());
    Assert.assertTrue(workUnits.size() == 1);

    //time stamps below corresponds to the date given in watermark fields in test data.
    Assert.assertEquals(GobblinMultiStageTestHelpers
            .getDateFromTimeStamp(
                Long.parseLong(workUnits.get(0).getLowWatermark().getAsJsonObject().get("value").toString())),
        "2019-08-01");
    Assert.assertEquals(GobblinMultiStageTestHelpers
            .getDateFromTimeStamp(
                Long.parseLong(workUnits.get(0).getExpectedHighWatermark().getAsJsonObject().get("value").toString())),
        "2019-08-02");
  }

  /*
   * precondition check failure test.
   */
  @Test(enabled=false)
  public void preConditionCheckFail() {
    boolean isIllegalState = false;
    try {
      HttpSource source = new HttpSource();
      SourceState state = GobblinMultiStageTestHelpers.prepareSourceStateWithWaterMark();
      when(state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY)).thenReturn(null);
      List<WorkUnit> workUnits = source.getWorkunits(state);
    } catch (Exception e) {
      isIllegalState = e.getClass().getCanonicalName()
          .contains("IllegalStateException");
    }
    Assert.assertTrue(isIllegalState);
  }

  @Test
  public void testGetAuthenticationHeader() {
    SourceState state = new SourceState();
    HttpSource httpSource = new HttpSource();
    state.setProp("source.conn.username", "1");
    state.setProp("source.conn.password", "2");

    state.setProp("ms.authentication", "{\"method\":\"basic\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Basic MToy}");

    state.setProp("ms.authentication", "{\"method\":\"bearer\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Bearer MToy}");

    state.setProp("ms.authentication", "{\"method\":\"bearer\",\"encryption\":\"base64\", \"header\": \"Authorization\", \"token\": \"xyz\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Bearer eHl6}");
  }

  /**
   * Test getAuthenticationHeader
   */
  @Test
  public void testGetAuthenticationHeader2() {
    PowerMockito.mockStatic(EncryptionUtils.class);

    HttpSourceKeys httpSourceKeys = mock(HttpSourceKeys.class);
    source.setHttpSourceKeys(httpSourceKeys);

    JsonObject authObj = gson.fromJson("{\"method\":\"some-method\",\"encryption\":\"base32\",\"header\":\"Authorization\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    Assert.assertEquals(source.getAuthenticationHeader(state), new HashMap<>());

    authObj = gson.fromJson("{\"method\":\"oauth\",\"encryption\":\"base32\",\"header\":\"Authorization\",\"token\":\"sdf23someresfsdwrw24234\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    String token = "someDecryptedToken";
    when(EncryptionUtils.decryptGobblin(any(), any())).thenReturn(token);
    Assert.assertEquals(source.getAuthenticationHeader(state).get("Authorization"), OAUTH_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token);

    authObj = gson.fromJson("{\"method\":\"custom\",\"encryption\":\"base32\",\"header\":\"Authorization\",\"token\":\"sdf23someresfsdwrw24234\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    Assert.assertEquals(source.getAuthenticationHeader(state).get("Authorization"), token);
  }

  /**
   * Test Execute
   * @throws IOException
   */
  @Test
  public void testExecute() throws IOException {
    initializeHelper();
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    source.httpClient = client;
    when(client.execute(any())).thenReturn(response);

    MultistageExtractor extractor = (MultistageExtractor) source.getExtractor(state);
    sourceKeys = source.getSourceKeys();
    WorkUnit workUnit = mock(WorkUnit.class);
    LongWatermark lowWatermark = mock(LongWatermark.class);
    LongWatermark highWatermark = mock(LongWatermark.class);

    long lowWaterMark = 1590994800000L; //2020-06-01
    long highWaterMark = 1591513200000L; //2020-06-07
    when(workUnit.getLowWatermark(LongWatermark.class)).thenReturn(lowWatermark);
    when(lowWatermark.getValue()).thenReturn(lowWaterMark);
    when(workUnit.getExpectedHighWatermark(LongWatermark.class)).thenReturn(highWatermark);
    when(highWatermark.getValue()).thenReturn(highWaterMark);
    when(state.getWorkunit()).thenReturn(workUnit);

    HttpRequestMethod command = mock(HttpRequestMethod.class);
    ConcurrentHashMap memberResponse = mock(ConcurrentHashMap.class);
    WorkUnitStatus status = mock(WorkUnitStatus.class);
    source.memberParameters = memberResponse;

    JsonObject parameters = new JsonObject();
    when(command.toString()).thenReturn("Some http method");
    when(memberResponse.get(extractor)).thenReturn(parameters);

    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(statusLine.getReasonPhrase()).thenReturn("reason1 for success");
    Assert.assertNotNull(source.execute(command, extractor, status));

    HttpEntity entity = mock(HttpEntity.class);
    Header header = mock(Header.class);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContentType()).thenReturn(header);

    HeaderElement element = mock(HeaderElement.class);
    when(header.getElements()).thenReturn(new HeaderElement[]{element});
    when(element.getName()).thenReturn("application/json");
    Assert.assertNotNull(source.execute(command, extractor, status));

    when(response.getEntity()).thenReturn(null);

    when(statusLine.getStatusCode()).thenReturn(204);
    Assert.assertNotNull(source.execute(command, extractor, status));

    when(statusLine.getStatusCode()).thenReturn(302);
    when(statusLine.getReasonPhrase()).thenReturn("reason1 for warning");
    Assert.assertNull(source.execute(command, extractor, status));

    when(statusLine.getStatusCode()).thenReturn(405);
    Assert.assertNull(source.execute(command, extractor, status));

    when(statusLine.getReasonPhrase()).thenReturn("reason1 for error");
    Assert.assertNull(source.execute(command, extractor, status));

    when(statusLine.getStatusCode()).thenReturn(408);
    Assert.assertNull(source.execute(command, extractor, status));

    when(response.getEntity()).thenReturn(entity);
    doThrow(new RuntimeException()).when(entity).getContentType();
    Assert.assertNull(source.execute(command, extractor, status));
  }

  /**
   * Test getNext
   */
  @Test
  public void testGetNext() {
    HttpSource httpSource = new HttpSource();
    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    when(sourceKeys.getCallInterval()).thenReturn(1L);
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);

    when(extractorKeys.getSignature()).thenReturn("testSignature");
    when(extractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(builder.build()).thenReturn(workUnitStatus);
    when(extractor.getExtractorKeys()).thenReturn(extractorKeys);
    when(extractor.getWorkUnitStatus()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);

    HttpSourceKeys httpSourceKeys = Mockito.mock(HttpSourceKeys.class);
    httpSource.setHttpSourceKeys(httpSourceKeys);
    when(httpSourceKeys.getHttpRequestMethod()).thenReturn("GET");

    Assert.assertNull(httpSource.getNext(extractor));
  }

  /**
   * Test closeStream
   */
  @Test
  public void testCloseStream() {
    HttpSource source = new HttpSource();
    MultistageExtractor extractor = mock(MultistageExtractor.class);
    ExtractorKeys keys = mock(ExtractorKeys.class);
    String testSignature = "test_signature";
    when(extractor.getExtractorKeys()).thenReturn(keys);
    when(keys.getSignature()).thenReturn(testSignature);

    source.closeStream(extractor);

    ConcurrentHashMap memberResponse = mock(ConcurrentHashMap.class);
    CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
    source.setMemberResponse(memberResponse);

    when(memberResponse.get(any())).thenReturn(httpResponse);
    source.closeStream(extractor);

    doThrow(new RuntimeException()).when(memberResponse).get(extractor);
    source.closeStream(extractor);
  }

  /**
   * Test getExtractor
   */
  @Test
  public void testGetExtractor() {
    initializeHelper();
    PowerMockito.mockStatic(EncryptionUtils.class);
    when(EncryptionUtils.decryptGobblin(token, state)).thenReturn(token);
    source.getExtractor(state);
    sourceKeys = source.getSourceKeys();
    Map<ParameterTypes, String> paginationFields = new HashMap<>();
    Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
    JsonArray fields = pagination.get("fields").getAsJsonArray();
    for (int i = 0; i < fields.size(); i++) {
      switch (fields.get(i).getAsString()) {
        case "page_start":
          paginationFields.put(ParameterTypes.PAGESTART, "page_start");
          break;
        case "page_size":
          paginationFields.put(ParameterTypes.PAGESIZE, "page_size");
          break;
        case "page_number":
          paginationFields.put(ParameterTypes.PAGENO, "page_number");
          break;
      }
    }

    JsonArray initialvalues = pagination.get("initialvalues").getAsJsonArray();
    for (int i = 0; i < initialvalues.size(); i++) {
      switch (i) {
        case 0:
          paginationInitValues.put(ParameterTypes.PAGESTART, initialvalues.get(0).getAsLong());
          break;
        case 1:
          paginationInitValues.put(ParameterTypes.PAGESIZE, initialvalues.get(1).getAsLong());
          break;
        case 2:
          paginationInitValues.put(ParameterTypes.PAGENO, initialvalues.get(2).getAsLong());
          break;
      }
    }

    Assert.assertEquals(sourceKeys.getPaginationFields(), paginationFields);
    Assert.assertEquals(sourceKeys.getPaginationInitValues(), paginationInitValues);
    Assert.assertEquals(sourceKeys.getSessionKeyField(), sessionKeyField);
    Assert.assertEquals(sourceKeys.getTotalCountField(), totalCountField);
    Assert.assertEquals(sourceKeys.getSourceParameters(), parameters);
    Assert.assertEquals(sourceKeys.getEncryptionField(), encryptionFields);
    Assert.assertEquals(sourceKeys.getDataField(), dataField);
    Assert.assertEquals(sourceKeys.getCallInterval(), callInterval.longValue());
    Assert.assertEquals(sourceKeys.getSessionTimeout(), waitTimeoutSeconds.longValue() * 1000);
    Assert.assertEquals(sourceKeys.getWatermarkDefinition(), watermark);
    Assert.assertEquals(sourceKeys.getSecondaryInputs(), secondaryInput);
    Assert.assertEquals(source.getHttpSourceKeys().getAuthentication(), authentication);
    Assert.assertEquals(source.getHttpSourceKeys().getHttpSourceUri(), sourceUri);
    Assert.assertEquals(source.getHttpSourceKeys().getHttpRequestMethod(), httpRequestMethod);

    Map<String, List<Integer>> httpStatuses = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : httpStatus.entrySet()) {
      String key = entry.getKey();
      List<Integer> codes = new ArrayList<>();
      for (int i = 0; i < entry.getValue().getAsJsonArray().size(); i++) {
        codes.add(entry.getValue().getAsJsonArray().get(i).getAsInt());
      }
      httpStatuses.put(key, codes);
    }
    Assert.assertEquals(source.getHttpSourceKeys().getHttpStatuses(), httpStatuses);

    Map<String, List<String>> StatusesReasons = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : httpStatusReasons.entrySet()) {
      String key = entry.getKey();
      List<String> reasons = new ArrayList<>();
      for (int i = 0; i < entry.getValue().getAsJsonArray().size(); i++) {
        reasons.add(entry.getValue().getAsJsonArray().get(i).getAsString());
      }
      StatusesReasons.put(key, reasons);
    }
    Assert.assertEquals(source.getHttpSourceKeys().getHttpStatusReasons(), StatusesReasons);
  }

  /**
   * Test shutdown
   */
  @Test
  public void testShutdown() throws IOException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    source.httpClient = client;

    doNothing().when((Closeable) client).close();
    source.shutdown(sourceState);

    doThrow(new RuntimeException()).when((Closeable) client).close();
    source.shutdown(sourceState);

    AutoRetryHttpClient retryHttpClient = mock(AutoRetryHttpClient.class);
    source.httpClient = retryHttpClient;
    source.shutdown(sourceState);
  }

  /**
   * Test getHttpStatuses
   */
  @Test
  public void testGetHttpStatuses() throws Exception {
    String statuses = "{\"success\":{\"someKey\":\"someValue\"},\"warning\":null}";
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(statuses);
    Assert.assertEquals(Whitebox.invokeMethod(source, "getHttpStatuses", state), new HashMap<>());
  }

  /**
   * Test getHttpStatusReasons
   */
  @Test
  public void testGetHttpStatusReasons() throws Exception {
    String reasons = "{\"success\":{\"someReason\":\"someValue\"},\"warning\":null}";
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(reasons);
    Assert.assertEquals(Whitebox.invokeMethod(source, "getHttpStatusReasons", state), new HashMap<>());
  }

  /**
   * Test getResponseContentType
   */
  @Test
  public void testGetResponseContentType() throws Exception {
    HttpResponse response = mock(HttpResponse.class);
    String methodName = "getResponseContentType";
    when(response.getEntity()).thenReturn(null);
    Assert.assertEquals(Whitebox.invokeMethod(source, methodName, response), StringUtils.EMPTY);

    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContentType()).thenReturn(null);
    Assert.assertEquals(Whitebox.invokeMethod(source, methodName, response), StringUtils.EMPTY);

    Header contentType = mock(Header.class);
    when(entity.getContentType()).thenReturn(contentType);

    HeaderElement[] headerElements = new HeaderElement[]{};
    when(contentType.getElements()).thenReturn(headerElements);
    Assert.assertEquals(Whitebox.invokeMethod(source, methodName, response), StringUtils.EMPTY);

    String type = "some_type";
    HeaderElement element = mock(HeaderElement.class);
    when(element.getName()).thenReturn(type);
    headerElements = new HeaderElement[]{element};
    when(contentType.getElements()).thenReturn(headerElements);
    Assert.assertEquals(Whitebox.invokeMethod(source, methodName, response), type);
  }

  private void initializeHelper() {
    JsonObject allKeys = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-data-for-source.json")), JsonObject.class);
    pagination = allKeys.get(MSTAGE_PAGINATION.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_PAGINATION.getConfig(), new JsonObject().toString())).thenReturn(pagination.toString());

    sessionKeyField = allKeys.get(MSTAGE_SESSION_KEY_FIELD.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_SESSION_KEY_FIELD.getConfig(), new JsonObject().toString())).thenReturn(sessionKeyField.toString());

    totalCountField = allKeys.get(MSTAGE_TOTAL_COUNT_FIELD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_TOTAL_COUNT_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(totalCountField);

    parameters = allKeys.get(MSTAGE_PARAMETERS.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_PARAMETERS.getConfig(), new JsonArray().toString())).thenReturn(parameters.toString());

    encryptionFields = allKeys.get(MSTAGE_ENCRYPTION_FIELDS.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_ENCRYPTION_FIELDS.getConfig(), new JsonArray().toString())).thenReturn(encryptionFields.toString());

    dataField = allKeys.get(MSTAGE_DATA_FIELD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_DATA_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(dataField);

    callInterval = allKeys.get(MSTAGE_CALL_INTERVAL.getConfig()).getAsLong();
    when(state.getPropAsLong(MSTAGE_CALL_INTERVAL.getConfig(), 0L)).thenReturn(callInterval);

    waitTimeoutSeconds = allKeys.get(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig()).getAsLong();
    when(state.getPropAsLong(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig(), 0L)).thenReturn(waitTimeoutSeconds);

    enableCleansing = allKeys.get(MSTAGE_ENABLE_CLEANSING.getConfig()).getAsBoolean();
    when(state.getPropAsBoolean(MSTAGE_ENABLE_CLEANSING.getConfig())).thenReturn(enableCleansing);

    workUnitPartialPartition = allKeys.get(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig()).getAsBoolean();
    when(state.getPropAsBoolean(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig())).thenReturn(workUnitPartialPartition);

    watermark = allKeys.get(MSTAGE_WATERMARK.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_WATERMARK.getConfig(), new JsonArray().toString())).thenReturn(watermark.toString());

    secondaryInput = allKeys.get(MSTAGE_SECONDARY_INPUT.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString())).thenReturn(secondaryInput.toString());

    httpRequestHeaders = allKeys.get(MSTAGE_HTTP_REQUEST_HEADERS.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_REQUEST_HEADERS.getConfig(), new JsonObject().toString())).thenReturn(httpRequestHeaders.toString());

    sourceUri = allKeys.get(MSTAGE_SOURCE_URI.getConfig()).getAsString();
    when(state.getProp(MSTAGE_SOURCE_URI.getConfig(), StringUtils.EMPTY)).thenReturn(sourceUri);

    httpRequestMethod = allKeys.get(MSTAGE_HTTP_REQUEST_METHOD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_HTTP_REQUEST_METHOD.getConfig(), StringUtils.EMPTY)).thenReturn(httpRequestMethod);

    extractorClass = allKeys.get(MSTAGE_EXTRACTOR_CLASS.getConfig()).getAsString();
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(extractorClass);

    authentication = allKeys.get(MSTAGE_AUTHENTICATION.getConfig()).getAsJsonObject();
    token = authentication.get("token").getAsString();
    when(state.getProp(MSTAGE_AUTHENTICATION.getConfig(), new JsonObject().toString())).thenReturn(authentication.toString());

    httpStatus = allKeys.get(MSTAGE_HTTP_STATUSES.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(httpStatus.toString());

    httpStatusReasons = allKeys.get(MSTAGE_HTTP_STATUS_REASONS.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(httpStatusReasons.toString());
  }
}