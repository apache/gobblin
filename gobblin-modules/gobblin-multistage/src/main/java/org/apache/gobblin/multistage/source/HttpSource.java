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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.connection.HttpReadConnection;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.keys.HttpKeys;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 *
 * HttpSource is a generic Gobblin source connector for HTTP based data sources including
 * Rest API
 *
 * @author chrli
 */
@Slf4j
@SuppressWarnings("unchecked")
public class HttpSource extends MultistageSource<Schema, GenericRecord> {
  private final static Gson GSON = new Gson();
  private final static String BASIC_TOKEN_PREFIX = "Basic";
  private final static String BEARER_TOKEN_PREFIX = "Bearer";
  final static String OAUTH_TOKEN_PREFIX = "OAuth";
  final static String TOKEN_PREFIX_SEPARATOR = " ";
  @VisibleForTesting

  @Getter @Setter
  private HttpKeys httpSourceKeys;

  public HttpSource() {
    httpSourceKeys = new HttpKeys();
    jobKeys = httpSourceKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    httpSourceKeys.logUsage(state);
    httpSourceKeys.setHttpRequestHeaders(getRequestHeader(state));
    httpSourceKeys.setHttpSourceUri(MultistageProperties.MSTAGE_SOURCE_URI.getProp(state));
    httpSourceKeys.setHttpRequestMethod(MultistageProperties.MSTAGE_HTTP_REQUEST_METHOD.getProp(state));
    httpSourceKeys.setAuthentication(MultistageProperties.MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state));
    httpSourceKeys.setHttpRequestHeadersWithAuthentication(getHeadersWithAuthentication(state));
    httpSourceKeys.setHttpStatuses(getHttpStatuses(state));
    httpSourceKeys.setHttpStatusReasons(getHttpStatusReasons(state));
    httpSourceKeys.logDebugAll();
  }
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    sourceState = state;
    initialize(state);
    return super.getWorkunits(state);
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new HttpConnection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */
  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new HttpReadConnection(state, this.httpSourceKeys, extractor.getExtractorKeys()));
    return extractor;
  }

  /**
   * Support:
   *   Basic Http Authentication
   *   Bearer token with Authorization header only, not including access_token in URI or Entity Body
   *
   * see Bearer token reference: https://tools.ietf.org/html/rfc6750
   *
   * TODO: add function to read authentication token from files
   *
   * @param state source state
   * @return header tag with proper encryption of tokens
   */
  @VisibleForTesting
  Map<String, String> getAuthenticationHeader(State state) {
    if (httpSourceKeys.getAuthentication().entrySet().size() == 0) {
      return new HashMap<>();
    }

    String authMethod = httpSourceKeys.getAuthentication().get("method").getAsString();
    if (!authMethod.toLowerCase().matches("basic|bearer|oauth|custom")) {
      log.warn("Unsupported authentication type: " + authMethod);
      return new HashMap<>();
    }

    String token = "";
    if (httpSourceKeys.getAuthentication().has("token")) {
      token = EncryptionUtils.decryptGobblin(httpSourceKeys.getAuthentication().get("token").getAsString(), state);
    } else {
      String u = EncryptionUtils.decryptGobblin(MultistageProperties.SOURCE_CONN_USERNAME.getProp(state), state);
      String p = EncryptionUtils.decryptGobblin(MultistageProperties.SOURCE_CONN_PASSWORD.getProp(state), state);
      token = u + ":" + p;
    }

    if (httpSourceKeys.getAuthentication().get("encryption").getAsString().equalsIgnoreCase("base64")) {
      token = new String(Base64.encodeBase64((token).getBytes()));
    }

    String header = "";
    if (authMethod.equalsIgnoreCase(BASIC_TOKEN_PREFIX)) {
      header = BASIC_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else if (authMethod.equalsIgnoreCase(BEARER_TOKEN_PREFIX)) {
      header = BEARER_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else if (authMethod.equalsIgnoreCase(OAUTH_TOKEN_PREFIX)) {
      header = OAUTH_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else {
      header = token;
    }
    return new ImmutableMap.Builder<String, String>().put(httpSourceKeys.getAuthentication().get("header").getAsString(), header).build();
  }

  private Map<String, String> getHeadersWithAuthentication(State state) {
    Map<String, String> headers = toStringStringMap(httpSourceKeys.getHttpRequestHeaders());
    headers.putAll(getAuthenticationHeader(state));
    return headers;
  }

  private Map<String, String> toStringStringMap(JsonObject json) {
    return GSON.fromJson(json.toString(),
        new TypeToken<HashMap<String, String>>() { }.getType());
  }

  private Map<String, List<Integer>> getHttpStatuses(State state) {
    Map<String, List<Integer>> statuses = new HashMap<>();
    JsonObject jsonObject = MultistageProperties.MSTAGE_HTTP_STATUSES.getValidNonblankWithDefault(state);
    for (Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = jsonObject.get(key);
      if (!value.isJsonNull() && value.isJsonArray()) {
        statuses.put(key, GSON.fromJson(value.toString(), new TypeToken<List<Integer>>() { }.getType()));
      }
    }
    return statuses;
  }

  private Map<String, List<String>> getHttpStatusReasons(State state) {
    Map<String, List<String>> reasons = new HashMap<>();
    JsonObject jsonObject = MultistageProperties.MSTAGE_HTTP_STATUS_REASONS.getValidNonblankWithDefault(state);
    for (Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = jsonObject.get(key);
      if (!value.isJsonNull() && value.isJsonArray()) {
        reasons.put(key, GSON.fromJson(value.toString(), new TypeToken<List<String>>() { }.getType()));
      }
    }
    return reasons;
  }

  /**
   * read the ms.http.request.headers and decrypt values if encrypted
   * @param state the source state
   * @return the decrypted http request headers
   */
  private JsonObject getRequestHeader(State state) {
    JsonObject headers = MultistageProperties.MSTAGE_HTTP_REQUEST_HEADERS.getValidNonblankWithDefault(state);
    JsonObject decrypted = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: headers.entrySet()) {
      String key = entry.getKey();
      decrypted.addProperty(key, EncryptionUtils.decryptGobblin(headers.get(key).getAsString(), state));
    }
    return decrypted;
  }
}