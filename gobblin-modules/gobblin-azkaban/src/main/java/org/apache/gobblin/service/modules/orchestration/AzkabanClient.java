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

package org.apache.gobblin.service.modules.orchestration;

import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple http based client that uses Ajax API to communicate with Azkaban server.
 *
 * @see <a href="https://azkaban.github.io/azkaban/docs/latest/#ajax-api">
 *   https://azkaban.github.io/azkaban/docs/latest/#ajax-api
 * </a>
 */
public class AzkabanClient implements Closeable {
  protected final String username;
  protected final String password;
  protected final String url;
  protected final long sessionExpireInMin; // default value is 12h.
  protected SessionManager sessionManager;
  protected String sessionId;
  protected long sessionCreationTime = 0;
  protected CloseableHttpClient httpClient;
  private ExecutorService executorService;
  private Closer closer = Closer.create();
  private Retryer<AzkabanClientStatus> retryer;
  private static Logger log = LoggerFactory.getLogger(AzkabanClient.class);
  private Duration requestTimeout;

  /**
   * Child class should have a different builderMethodName.
   */
  @Builder
  protected AzkabanClient(String username,
                          String password,
                          String url,
                          long sessionExpireInMin,
                          CloseableHttpClient httpClient,
                          SessionManager sessionManager,
                          ExecutorService executorService,
                          Duration requestTimeout)
      throws AzkabanClientException {
    this.username = username;
    this.password = password;
    this.url = url;
    this.sessionExpireInMin = sessionExpireInMin;
    this.httpClient = httpClient;
    this.sessionManager = sessionManager;
    this.executorService = executorService;
    this.requestTimeout = ObjectUtils.defaultIfNull(requestTimeout, Duration.ofSeconds(10));

    this.initializeClient();
    this.initializeSessionManager();
    this.intializeExecutorService();

    this.retryer = RetryerBuilder.<AzkabanClientStatus>newBuilder()
        .retryIfExceptionOfType(InvalidSessionException.class)
        .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(this.requestTimeout.toMillis(), TimeUnit.MILLISECONDS,
            this.executorService))
        .withWaitStrategy(WaitStrategies.exponentialWait(60, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
    try {
      this.sessionId = this.sessionManager.fetchSession();
    } catch (Exception e) {
      this.sessionId = null;
      this.sessionCreationTime = -1;
      log.error("Failed to fetch session in constructor due to: ", e);
      return;
    }
    this.sessionCreationTime = System.nanoTime();
  }

  private void initializeClient() throws AzkabanClientException {
    if (this.httpClient == null) {
      this.httpClient = createHttpClient();
      this.closer.register(this.httpClient);
    }
  }

  private void initializeSessionManager() {
    if (sessionManager == null) {
      this.sessionManager = new AzkabanSessionManager(this.httpClient,
                                                      this.url,
                                                      this.username,
                                                      this.password);
    }
  }

  private void intializeExecutorService() {
    if (this.executorService == null) {
      this.executorService = Executors.newFixedThreadPool(30);
    }
  }

  /**
   * Create a {@link CloseableHttpClient} used to communicate with Azkaban server.
   * Derived class can configure different http client by overriding this method.
   *
   * @return A closeable http client.
   */
  private CloseableHttpClient createHttpClient() throws AzkabanClientException {
    try {
    // SSLSocketFactory using custom TrustStrategy that ignores warnings about untrusted certificates
    // Self sign SSL
    SSLContextBuilder sslcb = new SSLContextBuilder();
    sslcb.loadTrustMaterial(null, (TrustStrategy) new TrustSelfSignedStrategy());
    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcb.build());

    HttpClientBuilder builder = HttpClientBuilder.create();
    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
          .setSocketTimeout((int) this.requestTimeout.toMillis())
          .setConnectTimeout((int) this.requestTimeout.toMillis())
          .setConnectionRequestTimeout((int) this.requestTimeout.toMillis())
          .build();

      builder.disableCookieManagement()
          .useSystemProperties()
          .setDefaultRequestConfig(requestConfig)
          .setConnectionManager(new BasicHttpClientConnectionManager())
          .setSSLSocketFactory(sslsf);

      return builder.build();
    } catch (Exception e) {
      throw new AzkabanClientException("HttpClient cannot be created", e);
    }
  }

  /**
   * When current session expired, use {@link SessionManager} to refresh the session id.
   */
  void refreshSession(boolean forceRefresh) throws AzkabanClientException {
    Preconditions.checkArgument(this.sessionCreationTime != 0);
    boolean expired = (System.nanoTime() - this.sessionCreationTime) > Duration
        .ofMinutes(this.sessionExpireInMin)
        .toNanos();

    if (expired) {
      log.debug("Session expired. Generating a new session.");
    } else if (forceRefresh) {
      log.info("Force to refresh session. Generating a new session.");
    }

    if (expired || forceRefresh) {
      this.sessionId = this.sessionManager.fetchSession();
      this.sessionCreationTime = System.nanoTime();
    }
  }

  /**
   * Convert a {@link HttpResponse} to a <string, string> map.
   * Put protected modifier here so it is visible to {@link AzkabanAjaxAPIClient}.
   *
   * @param response An http response returned by {@link org.apache.http.client.HttpClient} execution.
   *                 This should be JSON string.
   * @return A map composed by the first level of KV pair of json object
   */
  protected static Map<String, String> handleResponse(HttpResponse response) throws IOException {
    verifyStatusCode(response);
    JsonObject json = getResponseJson(response);
    return getFlatMap(json);
  }

  protected static <T> T handleResponse(HttpResponse response, Class<T> responseClass) throws IOException {
    verifyStatusCode(response);
    JsonObject json = getResponseJson(response);

    Gson gson = new Gson();
    return gson.fromJson(json, responseClass);
  }

  private static JsonObject getResponseJson(HttpResponse response) throws IOException {
    HttpEntity entity = null;
    String jsonResponseString;

    try {
      entity = response.getEntity();
      jsonResponseString = IOUtils.toString(entity.getContent(), "UTF-8");
      log.debug("Response string: {}", jsonResponseString);
    } catch (Exception e) {
      throw new AzkabanClientException("Cannot convert response to a string", e);
    } finally {
      if (entity != null) {
        EntityUtils.consume(entity);
      }
    }
    return parseResponse(jsonResponseString);
  }

  protected static void verifyStatusCode(HttpResponse response) throws AzkabanClientException {
    int code = response.getStatusLine().getStatusCode();
    if (code != HttpStatus.SC_CREATED && code != HttpStatus.SC_OK) {
      log.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
      throw new AzkabanClientException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
    }
  }

  static Map<String, String> getFlatMap(JsonObject jsonObject) {
    if (jsonObject == null) {
      return null;
    }
    Map<String, String> responseMap = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      responseMap.put(entry.getKey(), entry.getValue().toString().replaceAll("\"", ""));
    }
    return responseMap;
  }

  static JsonObject parseResponse(String jsonResponseString) throws IOException {
    if (!StringUtils.isNotBlank(jsonResponseString)) {
      return null;
    }
    JsonObject jsonObject = new JsonParser().parse(jsonResponseString).getAsJsonObject();
    handleResponseError(jsonObject);
    return jsonObject;
  }

  private static void handleResponseError(JsonObject jsonObject) throws IOException {
    // Azkaban does not has a standard for error messages tag
    if (null != jsonObject.get(AzkabanClientParams.STATUS) &&
        AzkabanClientParams.ERROR.equalsIgnoreCase(jsonObject.get(AzkabanClientParams.STATUS).toString()
        .replaceAll("\"", ""))) {
      String message = (null != jsonObject.get(AzkabanClientParams.MESSAGE)) ? jsonObject.get(AzkabanClientParams.MESSAGE).toString()
          .replaceAll("\"", "") : "Unknown issue";

      if (message.contains("Invalid Session")) {
        throw new InvalidSessionException(message);
      }

      throw new IOException(message);
    }

    if (null != jsonObject.get(AzkabanClientParams.ERROR)) {
      String error = jsonObject.get(AzkabanClientParams.ERROR).toString().replaceAll("\"", "");
      throw new AzkabanClientException(error);
    }
  }

  /**
   * Creates a project.
   *
   * @param projectName project name
   * @param description project description
   *
   * @return A status object indicating if AJAX request is successful.
   */
  public AzkabanClientStatus createProject(String projectName,
                                           String description) throws AzkabanClientException {
    AzkabanMultiCallables.CreateProjectCallable callable =
       AzkabanMultiCallables.CreateProjectCallable.builder()
           .client(this)
           .projectName(projectName)
           .description(description)
           .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Deletes a project. Currently no response message will be returned after finishing
   * the delete operation. Thus success status is always expected.
   *
   * @param projectName project name
   *
   * @return A status object indicating if AJAX request is successful.
   */
  public AzkabanClientStatus deleteProject(String projectName) throws AzkabanClientException {

    AzkabanMultiCallables.DeleteProjectCallable callable =
        AzkabanMultiCallables.DeleteProjectCallable.builder()
            .client(this)
            .projectName(projectName)
            .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Checks if the project with specified name exists in Azkaban
   */
  public Boolean projectExists(String projectName) throws AzkabanClientException {
    try {
      fetchProjectFlows(projectName);
      return true;
    } catch (AzkabanClientException e) {
      // Azkaban does not return a strongly typed error code, so we are checking the message
      if (e.getCause().getMessage().contains("doesn't exist")) {
        return false;
      } else {
        throw e;
      }
    }
  }
  /**
   * Updates a project by uploading a new zip file. Before uploading any project zip files,
   * the project should be created first.
   *
   * @param projectName project name
   * @param zipFile  zip file
   *
   * @return A status object indicating if AJAX request is successful.
   */
  public AzkabanClientStatus uploadProjectZip(String projectName,
                                              File zipFile) throws AzkabanClientException {

    AzkabanMultiCallables.UploadProjectCallable callable =
        AzkabanMultiCallables.UploadProjectCallable.builder()
            .client(this)
            .projectName(projectName)
            .zipFile(zipFile)
            .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Execute a flow by providing flow parameters and options. The project and flow should be created first.
   *
   * @param projectName project name
   * @param flowName  flow name
   * @param flowOptions  flow options
   * @param flowParameters  flow parameters
   *
   * @return The status object which contains success status and execution id.
   */
  public AzkabanExecuteFlowStatus executeFlowWithOptions(String projectName,
                                                         String flowName,
                                                         Map<String, String> flowOptions,
                                                         Map<String, String> flowParameters) throws AzkabanClientException {
    AzkabanMultiCallables.ExecuteFlowCallable callable =
        AzkabanMultiCallables.ExecuteFlowCallable.builder()
            .client(this)
            .projectName(projectName)
            .flowName(flowName)
            .flowOptions(flowOptions)
            .flowParameters(flowParameters)
            .build();

    return runWithRetry(callable, AzkabanExecuteFlowStatus.class);
  }

  /**
   * Execute a flow with flow parameters. The project and flow should be created first.
   *
   * @param projectName project name
   * @param flowName  flow name
   * @param flowParameters  flow parameters
   *
   * @return The status object which contains success status and execution id.
   */
  public AzkabanExecuteFlowStatus executeFlow(String projectName,
                                              String flowName,
                                              Map<String, String> flowParameters) throws AzkabanClientException {
    return executeFlowWithOptions(projectName, flowName, null, flowParameters);
  }

  /**
   * Cancel a flow by execution id.
   */
  public AzkabanClientStatus cancelFlow(String execId) throws AzkabanClientException {
    AzkabanMultiCallables.CancelFlowCallable callable =
        AzkabanMultiCallables.CancelFlowCallable.builder()
            .client(this)
            .execId(execId)
            .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Fetch an execution log.
   */
  public AzkabanClientStatus fetchExecutionLog(String execId,
                                               String jobId,
                                               long offset,
                                               long length,
                                               OutputStream logStream) throws AzkabanClientException {
    AzkabanMultiCallables.FetchExecLogCallable callable =
        AzkabanMultiCallables.FetchExecLogCallable.builder()
            .client(this)
            .execId(execId)
            .jobId(jobId)
            .offset(offset)
            .length(length)
            .output(logStream)
            .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Given an execution id, fetches all the detailed information of that execution,
   * including a list of all the job executions.
   *
   * @param execId execution id to be fetched.
   *
   * @return The status object which contains success status and all the detailed
   *         information of that execution.
   */
  public AzkabanFetchExecuteFlowStatus fetchFlowExecution(String execId) throws AzkabanClientException {
    AzkabanMultiCallables.FetchFlowExecCallable callable =
        AzkabanMultiCallables.FetchFlowExecCallable.builder()
            .client(this)
            .execId(execId)
            .build();

    return runWithRetry(callable, AzkabanFetchExecuteFlowStatus.class);
  }

  /**
   * Returns a list of flow ids in a specified project.
   *
   * @param projectName name of the project.
   */
  public AzkabanProjectFlowsStatus fetchProjectFlows(String projectName) throws AzkabanClientException {
    AzkabanMultiCallables.FetchProjectFlowsCallable callable =
            AzkabanMultiCallables.FetchProjectFlowsCallable.builder()
                    .client(this)
                    .projectName(projectName)
                    .build();

    return runWithRetry(callable, AzkabanProjectFlowsStatus.class);
  }

  /**
   * Given a project and user, add that user as a proxy user in the project.
   *
   * @param projectName project name
   * @param proxyUserName proxy user
   *
   * @return A status object indicating if AJAX request is successful.
   */
  public AzkabanClientStatus addProxyUser(String projectName, String proxyUserName) throws AzkabanClientException {
    AzkabanMultiCallables.AddProxyUserCallable callable = AzkabanMultiCallables.AddProxyUserCallable.builder()
        .client(this)
        .projectName(projectName)
        .proxyUserName(proxyUserName)
        .build();

    return runWithRetry(callable, AzkabanClientStatus.class);
  }

  /**
   * Get the list of proxy users for a given project.
   *
   * @param projectName project name
   *
   * @return {@link AzkabanGetProxyUsersStatus} containing the response map. The response should have a key "proxyUsers"
   * which will be in the format "[user1, user2, user3]"
   */
  public AzkabanGetProxyUsersStatus getProxyUsers(String projectName) throws AzkabanClientException {
    AzkabanMultiCallables.GetProxyUserCallable callable = AzkabanMultiCallables.GetProxyUserCallable.builder()
        .client(this)
        .projectName(projectName)
        .build();

    return runWithRetry(callable, AzkabanGetProxyUsersStatus.class);
  }

  private <T> T runWithRetry(Callable callable, Class<T> cls) throws AzkabanClientException {
    try {
      AzkabanClientStatus status = this.retryer.call(callable);
      if (cls.isAssignableFrom(status.getClass())) {
        return ((T)status);
      } else {
        throw new AzkabanClientException(String.format("Unexpected response type, expected: %s actual: %s", cls, status.getClass()));
      }
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), AzkabanClientException.class);
    } catch (RetryException e) {
      throw new AzkabanClientException("RetryException occurred ", e);
    }
    // should never reach to here.
    throw new UnreachableStatementException("Cannot reach here.");
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
