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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * A simple client that uses Ajax API to communicate with Azkaban server.
 *
 * Please note that this class can only be instantiated via {@link AzkabanClientBuilder}.
 *
 * @see {@linktourl https://azkaban.github.io/azkaban/docs/latest/#ajax-api}
 */
public class AzkabanClient implements Closeable {
  protected final String username;
  protected final String url;
  protected String password;
  protected String sessionId;
  protected CloseableHttpClient client;
  private static Logger log = LoggerFactory.getLogger(AzkabanClient.class);

  protected AzkabanClient(AzkabanClientBuilder builder) throws AzkabanClientException {
    this.url = builder.getUrl();
    this.username = builder.getUsername();
    this.password = builder.getPassword();
    this.client = getClient();
    this.initializeSession();
  }

  /**
   * Create a session id that can be used in the future to communicate with Azkaban server.
   */
  protected void initializeSession() throws AzkabanClientException {
    try {
      HttpPost httpPost = new HttpPost(this.url);
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair(AzkabanClientParams.ACTION, "login"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.USERNAME, this.username));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.PASSWORD, this.password));
      httpPost.setEntity(new UrlEncodedFormEntity(nvps));
      CloseableHttpResponse response = this.client.execute(httpPost);

      try {
        HttpEntity entity = response.getEntity();

        // retrieve session id from entity
        String jsonResponseString = IOUtils.toString(entity.getContent(), "UTF-8");
        this.sessionId = parseResponse(jsonResponseString).get(AzkabanClientParams.SESSION_ID);
        log.info("Session id : {}", this.sessionId);
        EntityUtils.consume(entity);
      } finally {
        response.close();
      }
    } catch (Exception e) {
      throw new AzkabanClientException("Azkaban client cannot initialize session.", e);
    }
  }

  /**
   * Create a {@link CloseableHttpClient} used to communicate with Azkaban server.
   * Derived class can configure different http client by overriding this method.
   *
   * @return A closeable http client.
   */
  protected CloseableHttpClient getClient() throws AzkabanClientException {
    try {
    // SSLSocketFactory using custom TrustStrategy that ignores warnings about untrusted certificates
    // Self sign SSL
    SSLContextBuilder sslcb = new SSLContextBuilder();
    sslcb.loadTrustMaterial(null, (TrustStrategy) new TrustSelfSignedStrategy());
    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcb.build());

    HttpClientBuilder builder = HttpClientBuilder.create();
    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
          .setSocketTimeout(10000)
          .setConnectTimeout(10000)
          .setConnectionRequestTimeout(10000)
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
   * Convert a {@link HttpResponse} to a <string, string> map.
   * Put protected modifier here so it is visible to {@link AzkabanAjaxAPIClient}.
   *
   * @param response An http response returned by {@link org.apache.http.client.HttpClient} execution.
   *                 This should be JSON string.
   * @return A map composed by the first level of KV pair of json object
   */
  protected static Map<String, String> handleResponse(HttpResponse response) throws IOException {
    int code = response.getStatusLine().getStatusCode();
    if (code != HttpStatus.SC_CREATED && code != HttpStatus.SC_OK) {
      log.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
      throw new AzkabanClientException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
    }

    // Get response in string
    HttpEntity entity = null;
    String jsonResponseString;

    try {
      entity = response.getEntity();
      jsonResponseString = IOUtils.toString(entity.getContent(), "UTF-8");
      log.info("Response string: " + jsonResponseString);
    } catch (Exception e) {
      throw new AzkabanClientException("Cannot convert response to a string", e);
    } finally {
      if (entity != null) {
        EntityUtils.consume(entity);
      }
    }

    return AzkabanClient.parseResponse(jsonResponseString);
  }

  private static Map<String, String> parseResponse(String jsonResponseString) throws IOException {
    // Parse Json
    Map<String, String> responseMap = new HashMap<>();
    if (StringUtils.isNotBlank(jsonResponseString)) {
      JsonObject jsonObject = new JsonParser().parse(jsonResponseString).getAsJsonObject();

      // Handle error if any
      handleResponseError(jsonObject);

      // Get all responseKeys
      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        responseMap.put(entry.getKey(), entry.getValue().toString().replaceAll("\"", ""));
      }
    }
    return responseMap;
  }

  private static void handleResponseError(JsonObject jsonObject) throws IOException {
    // Azkaban does not has a standard for error messages tag
    if (null != jsonObject.get(AzkabanClientParams.STATUS) &&
        AzkabanClientParams.ERROR.equalsIgnoreCase(jsonObject.get(AzkabanClientParams.STATUS).toString()
        .replaceAll("\"", ""))) {
      String message = (null != jsonObject.get(AzkabanClientParams.MESSAGE)) ? jsonObject.get(AzkabanClientParams.MESSAGE).toString()
          .replaceAll("\"", "") : "Unknown issue";
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
  public AzkabanClientStatus createProject(
      String projectName,
      String description) {
    try {
      HttpPost httpPost = new HttpPost(this.url + "/manager");
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair(AzkabanClientParams.ACTION, "create"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, this.sessionId));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.NAME, projectName));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.DESCRIPTION, description));
      httpPost.setEntity(new UrlEncodedFormEntity(nvps));

      Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
      Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");
      httpPost.setHeaders(new Header[]{contentType, requestType});

      CloseableHttpResponse response = this.client.execute(httpPost);

      try {
        handleResponse(response);
        return new AzkabanClientStatus.SUCCESS();
      } finally {
        response.close();
      }
    } catch (Exception e) {
      return new AzkabanClientStatus.FAIL("Azkaban client cannot create project.", e);
    }
  }

  /**
   * Deletes a project. Currently no response message will be returned after finishing
   * the delete operation. Thus success status is always expected.
   *
   * @param projectName project name
   *
   * @return A status object indicating if AJAX request is successful.
   */
  public AzkabanClientStatus deleteProject(String projectName) {
    try {
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair("delete", "true"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, this.sessionId));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));

      Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
      Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

      HttpGet httpGet = new HttpGet(url + "/manager?" + URLEncodedUtils.format(nvps, "UTF-8"));
      httpGet.setHeaders(new Header[]{contentType, requestType});

      CloseableHttpResponse response = this.client.execute(httpGet);
      response.close();
      return new AzkabanClientStatus.SUCCESS();

    } catch (Exception e) {
      return new AzkabanClientStatus.FAIL("Azkaban client cannot delete project = "
          + projectName, e);
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
  public AzkabanClientStatus uploadProjectZip(
      String projectName,
      File zipFile) {
    try {
      HttpPost httpPost = new HttpPost(this.url + "/manager");

      HttpEntity entity = MultipartEntityBuilder.create()
          .addTextBody(AzkabanClientParams.SESSION_ID, sessionId)
          .addTextBody(AzkabanClientParams.AJAX, "upload")
          .addTextBody(AzkabanClientParams.PROJECT, projectName)
          .addBinaryBody("file", zipFile,
              ContentType.create("application/zip"), zipFile.getName())
          .build();
      httpPost.setEntity(entity);

      CloseableHttpResponse response = this.client.execute(httpPost);

      try {
        handleResponse(response);
        return new AzkabanClientStatus.SUCCESS();
      } finally {
        response.close();
      }
    } catch (Exception e) {
      return new AzkabanClientStatus.FAIL("Azkaban client cannot upload zip to project = "
          + projectName, e);
    }
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
  public AzkabanExecuteFlowStatus executeFlowWithOptions(
      String projectName,
      String flowName,
      Map<String, String> flowOptions,
      Map<String, String> flowParameters) {
    try {
      HttpPost httpPost = new HttpPost(this.url + "/executor");
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "executeFlow"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, this.sessionId));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.PROJECT, projectName));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.FLOW, flowName));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.CONCURRENT_OPTION, "ignore"));

      addFlowOptions(nvps, flowOptions);
      addFlowParameters(nvps, flowParameters);

      httpPost.setEntity(new UrlEncodedFormEntity(nvps));

      Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
      Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");
      httpPost.setHeaders(new Header[]{contentType, requestType});

      CloseableHttpResponse response = this.client.execute(httpPost);

      try {
        Map<String, String> map = handleResponse(response);
        return new AzkabanExecuteFlowStatus(
            new AzkabanExecuteFlowStatus.ExecuteId(map.get(AzkabanClientParams.EXECID)));
      } finally {
        response.close();
      }
    } catch (Exception e) {
      return new AzkabanExecuteFlowStatus("Azkaban client cannot execute flow = "
          + flowName, e);
    }
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
  public AzkabanExecuteFlowStatus executeFlow(
      String projectName,
      String flowName,
      Map<String, String> flowParameters) {
    return executeFlowWithOptions(projectName, flowName, null, flowParameters);
  }

  /**
   * Given an execution id, fetches all the detailed information of that execution, including a list of all the job executions.
   *
   * @param execId execution id to be fetched.
   *
   * @return The status object which contains success status and all the detailed information of that execution.
   */
  public AzkabanFetchExecuteFlowStatus fetchFlowExecution (String execId) {
    try {
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair(AzkabanClientParams.AJAX, "fetchexecflow"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.SESSION_ID, this.sessionId));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.EXECID, execId));

      Header contentType = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
      Header requestType = new BasicHeader("X-Requested-With", "XMLHttpRequest");

      HttpGet httpGet = new HttpGet(url + "/executor?" + URLEncodedUtils.format(nvps, "UTF-8"));
      httpGet.setHeaders(new Header[]{contentType, requestType});

      CloseableHttpResponse response = this.client.execute(httpGet);
      try {
        Map<String, String> map = handleResponse(response);
        return new AzkabanFetchExecuteFlowStatus(new AzkabanFetchExecuteFlowStatus.Execution(map));
      } finally {
        response.close();
      }
    } catch (Exception e) {
      return new AzkabanFetchExecuteFlowStatus("Azkaban client cannot "
          + "fetch execId " + execId, e);
    }
  }

  private void addFlowParameters(List<NameValuePair> nvps, Map<String, String> flowParams) {
    if (flowParams != null) {
      for (Map.Entry<String, String> entry : flowParams.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
          log.debug("New flow parameter added:" + key + "-->" + value);
          nvps.add(new BasicNameValuePair("flowOverride[" + key + "]", value));
        }
      }
    }
  }

  private void addFlowOptions(List<NameValuePair> nvps, Map<String, String> flowOptions) {
    if (flowOptions != null) {
      for (Map.Entry<String, String> option : flowOptions.entrySet()) {
        log.debug("New flow option added:" + option .getKey()+ "-->" + option.getValue());
        nvps.add(new BasicNameValuePair(option.getKey(), option.getValue()));
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    this.client.close();
  }
}
