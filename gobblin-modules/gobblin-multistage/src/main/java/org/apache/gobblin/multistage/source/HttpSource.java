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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.factory.HttpClientFactory;
import org.apache.gobblin.multistage.keys.HttpSourceKeys;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.multistage.util.HttpRequestMethod;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;


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
  private final static String KEY_WORD_HTTP_OK = "ok";
  private final static String KEY_WORD_HTTP_NOTOK = "notok";
  private final static Gson GSON = new Gson();
  private final static String BASIC_TOKEN_PREFIX = "Basic";
  private final static String BEARER_TOKEN_PREFIX = "Bearer";
  final static String OAUTH_TOKEN_PREFIX = "OAuth";
  final static String TOKEN_PREFIX_SEPARATOR = " ";
  @VisibleForTesting
  HttpClient httpClient = null;
  @Getter
  @Setter
  private HttpSourceKeys httpSourceKeys = new HttpSourceKeys();
  @Setter
  private ConcurrentMap<MultistageExtractor, CloseableHttpResponse> memberResponse = new ConcurrentHashMap<>();

  protected void initialize(State state) {
    super.initialize(state);
    httpClient = getHttpClient(state);
    httpSourceKeys.setHttpRequestHeaders(getRequestHeader(state));
    httpSourceKeys.setHttpSourceUri(MultistageProperties.MSTAGE_SOURCE_URI.getProp(state));
    httpSourceKeys.setHttpRequestMethod(MultistageProperties.MSTAGE_HTTP_REQUEST_METHOD.getProp(state));
    httpSourceKeys.setAuthentication(MultistageProperties.MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state));
    httpSourceKeys.setHttpRequestHeadersWithAuthentication(getHeadersWithAuthentication(state));
    httpSourceKeys.setHttpStatuses(getHttpStatuses(state));
    httpSourceKeys.setHttpStatusReasons(getHttpStatusReasons(state));
    httpSourceKeys.logDebugAll();
    sourceKeys.logDebugAll();
    log.debug("Current Object: {}, {}", this.getClass(), this);
  }
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    sourceState = state;
    initialize(state);
    return super.getWorkunits(state);
  }

  @Override
  public WorkUnitStatus getFirst(MultistageExtractor extractor) {
    WorkUnitStatus status = super.getFirst(extractor);
    return execute(HttpRequestMethod.valueOf(httpSourceKeys.getHttpRequestMethod()),
        extractor, status);
  }

  @Override
  public WorkUnitStatus getNext(MultistageExtractor extractor) {
    WorkUnitStatus status = super.getNext(extractor);
    return execute(HttpRequestMethod.valueOf(httpSourceKeys.getHttpRequestMethod()),
        extractor, status);
  }

  @Override
  public void closeStream(MultistageExtractor extractor) {
    log.info("Closing InputStream for {}", extractor.getExtractorKeys().getSignature());
    try {
      if (memberResponse.get(extractor) != null) {
        memberResponse.get(extractor).close();
      } else {
        log.warn("Member response doesn't exist! The size of the cache is: {}", memberResponse.size());
      }
    } catch (Exception e) {
      log.warn("Error closing the input stream", e);
    }
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) {
    initialize(state);
    return super.getExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    try {
      if (this.httpClient instanceof Closeable) {
        ((Closeable) this.httpClient).close();
      }
    } catch (Exception e) {
      log.error("error closing HttpSource {}", e.getMessage());
    }
  }

  /**
   * Thread-safely create HttpClient as needed
   */
  synchronized HttpClient getHttpClient(State state) {
    if (httpClient == null) {
      try {
        Class factoryClass = Class.forName(
            MultistageProperties.MSTAGE_HTTP_CLIENT_FACTORY.getValidNonblankWithDefault(state));
        HttpClientFactory factory = (HttpClientFactory) factoryClass.newInstance();
        httpClient = factory.get(state);
      } catch (Exception e) {
        log.error("Error creating HttpClient: {}", e.getMessage());
      }
    }
    return httpClient;
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
      String u = EncryptionUtils.decryptGobblin(state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME, ""), state);
      String p = EncryptionUtils.decryptGobblin(state.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD, ""), state);
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

  @VisibleForTesting
  @SneakyThrows
  WorkUnitStatus execute(HttpRequestMethod command,
      MultistageExtractor extractor, WorkUnitStatus status) {
    Preconditions.checkNotNull(status, "WorkUnitStatus is not initialized.");
    CloseableHttpResponse response;
    try {
      response = retryExecuteHttpRequest(command,
          extractor.getState(),
          memberParameters.get(extractor),
          sourceKeys.getRetryCount());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return null;
    }

    // if no exception (error), but warnings, return work unit status as it was
    // this will be treated as "request was successful but don't process data records"
    if (response == null) {
      return status;
    }

    // even no error, no warning, we still need to process potential silent failures
    memberResponse.put(extractor, response);
    try {
      status.getMessages().put("contentType", getResponseContentType(response));
      status.getMessages().put("headers", getResponseHeaders(response).toString());
      if (response.getEntity() != null) {
        status.setBuffer(response.getEntity().getContent());
      }
    } catch (Exception e) {
      // Log but ignore errors when getting content and content type
      // These errors will lead to a NULL buffer in work unit status
      // And that situation will be handled in extractor accordingly
      log.error(e.getMessage());
    }

    return status;
  }

  @SneakyThrows
  private CloseableHttpResponse retryExecuteHttpRequest(
      final HttpRequestMethod command,
      final State state,
      final JsonObject parameters,
      long retry) {

    log.debug("Execute Http {} with parameters: {}", command.toString(), parameters.toString());
    Pair<String, CloseableHttpResponse> response = executeHttpRequest(command,
        httpSourceKeys.getHttpSourceUri(),
        parameters,
        httpSourceKeys.getHttpRequestHeadersWithAuthentication());

    if (response.getLeft().equalsIgnoreCase(KEY_WORD_HTTP_OK)) {
      log.info("Request was successful, return HTTP response");
      return response.getRight();
    }

    Integer status = response.getRight().getStatusLine().getStatusCode();

    // treat as warning if:
    // status is < 400, and not in error list
    // or status is in warning list
    // by returning NULL, the task will complete without failure
    if (status < 400 && !httpSourceKeys.getHttpStatuses().getOrDefault("error", Lists.newArrayList()).contains(status)
        || httpSourceKeys.getHttpStatuses().getOrDefault("warning", Lists.newArrayList()).contains(status)) {
      log.warn("Request was successful with warnings, return NULL response");
      return null;
    }

    // checks if there is an error related to retrieving the access token or
    // whether it has expired between pagination
    List<Integer> paginationErrors = httpSourceKeys.getHttpStatuses().getOrDefault(
        "pagination_error", Lists.newArrayList());
    while (isSecondaryAuthenticationEnabled() && retry > 0 && paginationErrors.contains(status)) {
      log.info("Request was unsuccessful, and needed retry with new authentication credentials");
      log.info("Sleep {} seconds, waiting for credentials to refresh", sourceKeys.getRetryDelayInSec());
      TimeUnit.SECONDS.sleep(sourceKeys.getRetryDelayInSec());

      // this might just read the stale old token, therefore need subsequent process
      JsonObject authentication = readSecondaryAuthentication(state, 0);
      if (JsonUtils.contains(parameters, authentication)) {
        log.info("Authentication credentials has not changed");
        log.info("Retrying getting new authentication credentials, retries: {}", retry - 1);
        retry--;
      } else {
        return retryExecuteHttpRequest(command, state,
            JsonUtils.replace(parameters, authentication), retry - 1);
      }
    }

    // every other error that should fail the job
    throw new RuntimeException("Error in executing HttpRequest: " + status.toString());
  }

  /**
   * Execute the request and return the response when everything goes OK, or null when
   * there are warnings, or raising runtime exception if any error.
   *
   * Successful if the response status code is one of the codes in ms.http.statuses.success and
   * the response status reason is not one of the codes in ms.http.status.reasons.error.
   *
   * Warning means the response cannot be process by the Extractor, and the task need to
   * terminate, but it should not fail the job. Status codes below 400 are considered as warnings
   * in general, but exceptions can be made by putting 4XX or 5XX codes in ms.http.statuses.warning
   * configuration.
   *
   * Error means the response cannot be process by the Extractor, and the task need to be terminated,
   * and the job should fail. Status codes 400 and above are considered as errors in general, but
   * exceptions can be made by putting 4XX or 5XX codes in ms.http.statuses.success or ms.http.statuses.warning,
   * or by putting 2XX and 3XX codes in ms.http.statuses.error.
   *
   * @param command the HttpRequestMethod object
   * @param httpUriTemplate the Uri template
   * @param parameters Http Request parameters
   * @param headers additional Http Request headers
   * @return a overall status and response pair, the overall status will be OK if status code is one of the
   * success status codes, anything else, including warnings, are considered as NOT OK
   */
  private Pair<String, CloseableHttpResponse> executeHttpRequest(final HttpRequestMethod command,
      final String httpUriTemplate, final JsonObject parameters, final Map<String, String> headers) {
    // trying to make a Http request, capture the client side error and
    // fail the task if any encoding exception or IO exception
    CloseableHttpResponse response;
    try {
      response = (CloseableHttpResponse) httpClient.execute(
          command.getHttpRequest(httpUriTemplate, parameters, headers));
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    // fail the task if response object is null
    Preconditions.checkNotNull(response, "Error in executing HttpRequest: response is null");

    // only pass the response stream to extractor when the status code and reason code all
    // indicate a success or there is a pagination error i.e. token has expired in between the pagination calls (in that
    // it will retry accessing the token by passing the response object back).
    Integer status = response.getStatusLine().getStatusCode();
    String reason = response.getStatusLine().getReasonPhrase();
    log.info("processing status: {} and reason: {}", status, reason);
    if (httpSourceKeys.getHttpStatuses().getOrDefault("success", Lists.newArrayList()).contains(status)
        && !httpSourceKeys.getHttpStatusReasons().getOrDefault("error", Lists.newArrayList()).contains(reason)) {
      log.info("Request was successful, returning OK and HTTP response.");
      return Pair.of(KEY_WORD_HTTP_OK, response);
    }

    // trying to consume the response stream and close it,
    // and fail the job if IOException happened during the process
    if (null != response.getEntity()) {
      try {
        reason += StringUtils.LF + EntityUtils.toString(response.getEntity());
        log.error("Status code: {}, reason: {}", status, reason);
        response.close();
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    log.warn("Request was unsuccessful, returning NOTOK and HTTP response");
    return Pair.of(KEY_WORD_HTTP_NOTOK, response);
  }

  @Override
  protected void logUsage(State state) {
    super.logUsage(state);
    List<MultistageProperties> list = httpSourceKeys.getEssentialParameters();
    for (MultistageProperties p: list) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
    httpSourceKeys.logDebugAll();
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

  /**
   * Get the content type string from response
   * @param response HttpResponse
   * @return the content type if available, otherwise, an empty string
   */
  private String getResponseContentType(HttpResponse response) {
    if (response.getEntity() != null
        && response.getEntity().getContentType() != null) {
      HeaderElement[] headerElements = response.getEntity().getContentType().getElements();
      if (headerElements.length > 0) {
        return headerElements[0].getName();
      }
    }
    return StringUtils.EMPTY;
  }

  /**
   * Get all headers from response
   * @param response HttpResponse
   * @return the headers in a JsonObject format, otherwise, an empty JsonObject
   */
  private JsonObject getResponseHeaders(HttpResponse response) {
    JsonObject headers = new JsonObject();
    if (response.getAllHeaders() != null) {
      for (Header header : response.getAllHeaders()) {
        headers.addProperty(header.getName(), header.getValue());
      }
    }
    return headers;
  }
}