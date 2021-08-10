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
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.Path;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.gobblin.multistage.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@PrepareForTest({VariableUtils.class, MultistageSource.class})
public class MultistageSource2Test extends PowerMockTestCase {

  private final static String UTF_8_CHARACTER_ENCODING = StandardCharsets.UTF_8.toString();
  private MultistageSource source;
  private SourceState state;
  private Gson gson;
  @BeforeClass
  public void setUp() {
    source = new MultistageSource();
    state = mock(SourceState.class);
    gson = new Gson();
  }

  @Test
  public void testDecode() throws UnsupportedEncodingException {
    PowerMockito.mockStatic(URLDecoder.class);
    String encoded = "test_encoded_string";
    when(URLDecoder.decode(encoded, UTF_8_CHARACTER_ENCODING)).thenThrow(UnsupportedEncodingException.class);
    Assert.assertEquals(source.decode(encoded), encoded);
  }

  @Test
  public void testGetEncodedUtf8() throws Exception {
    PowerMockito.mockStatic(URLEncoder.class);
    String plainUrl = "plain_url";
    when(URLEncoder.encode(plainUrl, UTF_8_CHARACTER_ENCODING)).thenThrow(UnsupportedEncodingException.class);
    Assert.assertEquals(source.getEncodedUtf8(plainUrl), plainUrl);
  }

  @Test
  public void testGetHadoopFsEncoded() throws Exception {
    PowerMockito.mockStatic(URLEncoder.class);
    String fileName = "file_name";
    PowerMockito.doThrow(new UnsupportedEncodingException()).when(URLEncoder.class, "encode", Path.SEPARATOR, StandardCharsets.UTF_8.toString());
    Assert.assertEquals(source.getHadoopFsEncoded(fileName), fileName);
  }

  @Test
  public void testGetHadoopFsDecoded() throws Exception {
    String encodedFileName = "dir%2FfileName";
    Assert.assertEquals(source.getHadoopFsDecoded(encodedFileName), "dir/fileName");

    PowerMockito.mockStatic(URLEncoder.class);
    PowerMockito.doThrow(new UnsupportedEncodingException()).when(URLEncoder.class, "encode", Path.SEPARATOR, StandardCharsets.UTF_8.toString());
    Assert.assertEquals(source.getHadoopFsDecoded(encodedFileName), encodedFileName);
  }

  @Test
  public void testGetWorkUnitSpecificString() throws UnsupportedEncodingException {
    PowerMockito.mockStatic(VariableUtils.class);
    String template = "test_template";
    JsonObject obj = new JsonObject();
    when(VariableUtils.replaceWithTracking(template, obj, false)).thenThrow(UnsupportedEncodingException.class);
    Assert.assertEquals(source.getWorkUnitSpecificString(template, obj), template);
  }

  /**
   * ReplaceVariablesInParameters() replace placeholders with their real values. This process
   * is called substitution.
   *
   * When the substituted parameter starts with tmp, the parameter is removed from the final.
   *
   * @throws Exception
   */
  @Test
  public void testReplaceVariablesInParameters() throws Exception {
    source = new MultistageSource();
    JsonObject parameters = gson.fromJson("{\"param1\":\"value1\"}", JsonObject.class);
    JsonObject replaced = source.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters);

    parameters = gson.fromJson("{\"param1\":\"value1\",\"param2\":\"{{param1}}\"}", JsonObject.class);
    JsonObject parameters2Expected = gson.fromJson("{\"param1\":\"value1\",\"param2\":\"value1\"}", JsonObject.class);
    replaced = source.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters2Expected);

    parameters = gson.fromJson("{\"tmpParam1\":\"value1\",\"param2\":\"{{tmpParam1}}\"}", JsonObject.class);
    parameters2Expected = gson.fromJson("{\"param2\":\"value1\"}", JsonObject.class);
    replaced = source.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters2Expected);
  }

  @Test
  public void testInitialize() {
    initializeHelper(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("true");
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString()))
        .thenReturn("[{\"fields\":[\"uuid\"],\"category\":\"authentication\",\"authentication\":{}}]");
    source.initialize(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("");
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString()))
        .thenReturn("[{\"path\":\"${job.dir}/${extract.namespace}/getResults\",\"fields\":[\"access_token\"],\"category\":\"authentication\",\"retry\":{}}]");
    source.initialize(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    source.initialize(state);
  }

  private void initializeHelper(SourceState state) {
    JsonObject allKeys = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-data-for-source.json")), JsonObject.class);

    when(state.getProp(MSTAGE_PAGINATION.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_PAGINATION.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_SESSION_KEY_FIELD.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_SESSION_KEY_FIELD.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_TOTAL_COUNT_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_TOTAL_COUNT_FIELD.getConfig()).getAsString());
    when(state.getProp(MSTAGE_PARAMETERS.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_PARAMETERS.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_ENCRYPTION_FIELDS.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_ENCRYPTION_FIELDS.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_DATA_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_DATA_FIELD.getConfig()).getAsString());
    when(state.getPropAsLong(MSTAGE_CALL_INTERVAL.getConfig(), 0L)).thenReturn(allKeys.get(MSTAGE_CALL_INTERVAL.getConfig()).getAsLong());
    when(state.getPropAsLong(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig(), 0L)).thenReturn(allKeys.get(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig()).getAsLong());
    when(state.getPropAsBoolean(MSTAGE_ENABLE_CLEANSING.getConfig())).thenReturn(allKeys.get(MSTAGE_ENABLE_CLEANSING.getConfig()).getAsBoolean());
    when(state.getPropAsBoolean(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig())).thenReturn(allKeys.get(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig()).getAsBoolean());
    when(state.getProp(MSTAGE_WATERMARK.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_WATERMARK.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_SECONDARY_INPUT.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_HTTP_REQUEST_HEADERS.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_REQUEST_HEADERS.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_SOURCE_URI.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_SOURCE_URI.getConfig()).getAsString());
    when(state.getProp(MSTAGE_HTTP_REQUEST_METHOD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_HTTP_REQUEST_METHOD.getConfig()).getAsString());
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_EXTRACTOR_CLASS.getConfig()).getAsString());
    when(state.getProp(MSTAGE_AUTHENTICATION.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_AUTHENTICATION.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_STATUSES.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_STATUS_REASONS.getConfig()).getAsJsonObject().toString());

    when(state.getProp(MSTAGE_SOURCE_S3_PARAMETERS.getConfig(), new JsonObject().toString())).thenReturn("{\"region\" : \"us-east-1\", \"connection_timeout\" : 10}");
    when(state.getProp(MSTAGE_SOURCE_FILES_PATTERN.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getPropAsInt(MSTAGE_S3_LIST_MAX_KEYS.getConfig())).thenReturn(100);
    when(state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME, StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD, StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_NAME.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), StringUtils.EMPTY)).thenReturn("");
  }
}