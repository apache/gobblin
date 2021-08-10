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

package org.apache.gobblin.multistage.connection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.exception.RetriableAuthenticationException;
import org.apache.gobblin.multistage.factory.HttpClientFactory;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.HttpKeys;
import org.apache.gobblin.multistage.keys.JobKeys;
import org.apache.gobblin.multistage.util.HttpRequestMethod;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;


@Slf4j
public class HttpReadConnection extends MultistageReadConnection {
  private final static String KEY_WORD_HTTP_OK = "ok";
  private final static String KEY_WORD_HTTP_NOTOK = "notok";
  @Getter (AccessLevel.PACKAGE)
  final private HttpKeys httpSourceKeys;

  @Setter (AccessLevel.PACKAGE)
  private HttpClient httpClient;
  @Setter (AccessLevel.PACKAGE)
  private CloseableHttpResponse response;

  public HttpReadConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    httpClient = getHttpClient(state);
    httpSourceKeys = (HttpKeys) jobKeys;
  }

  @Override
  public WorkUnitStatus execute(WorkUnitStatus status) throws RetriableAuthenticationException {
    return execute(HttpRequestMethod.valueOf(httpSourceKeys.getHttpRequestMethod()), status);
  }

  /**
   * Thread-safely create HttpClient as needed. This connection object
   * is mostly going to be initialized in separate threads; therefore,
   * this is more of a precaution.
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

  @Override
  public WorkUnitStatus getFirst(WorkUnitStatus workUnitStatus) throws  RetriableAuthenticationException {
    WorkUnitStatus status = super.getFirst(workUnitStatus);
    return execute(status);
  }

  @Override
  public WorkUnitStatus getNext(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.getNext(workUnitStatus);
    return execute(status);
  }

  @VisibleForTesting
  @SneakyThrows
  WorkUnitStatus execute(HttpRequestMethod command, WorkUnitStatus status) throws RetriableAuthenticationException {
    Preconditions.checkNotNull(status, "WorkUnitStatus is not initialized.");
    try {
      response = retryExecuteHttpRequest(command,
          getExtractorKeys().getDynamicParameters());
    } catch (RetriableAuthenticationException e) {
      throw e;
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
      final JsonObject parameters
  ) throws RetriableAuthenticationException {

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
    if (getJobKeys().getIsSecondaryAuthenticationEnabled() && paginationErrors.contains(status)) {
      log.info("Request was unsuccessful, and needed retry with new authentication credentials");
      log.info("Sleep {} seconds, waiting for credentials to refresh", getJobKeys().getRetryDelayInSec());
      throw new RetriableAuthenticationException("Stale authentication token.");
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

  @Override
  public boolean closeStream() {
    log.info("Closing InputStream for {}", getExtractorKeys().getSignature());
    try {
      if (response != null) {
        response.close();
      }
    } catch (Exception e) {
      log.warn("Error closing the input stream", e);
      return false;
    }
    return true;
  }


  @Override
  public boolean closeAll(String message) {
    try {
      if (this.httpClient instanceof Closeable) {
        ((Closeable) this.httpClient).close();
        httpClient = null;
      }
    } catch (Exception e) {
      log.error("error closing HttpSource {}", e.getMessage());
      return false;
    }
    return true;
  }
}
