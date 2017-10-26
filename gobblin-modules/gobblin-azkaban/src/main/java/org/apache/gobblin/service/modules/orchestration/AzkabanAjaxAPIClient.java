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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


@Slf4j
public class AzkabanAjaxAPIClient {
  private static Splitter SPLIT_ON_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();

  // TODO: Ensure GET call urls do not grow too big
  private static final int LOW_NETWORK_TRAFFIC_BEGIN_HOUR = 17;
  private static final int LOW_NETWORK_TRAFFIC_END_HOUR = 22;
  private static final int JOB_START_DELAY_MINUTES = 5;
  private static final long MILLISECONDS_IN_HOUR = 60 * 60 * 1000;
  private static final URLCodec codec = new URLCodec();

  /***
   * Authenticate a user and obtain a session.id from response. Once a session.id has been obtained,
   * until the session expires, this id can be used to do any API requests with a proper permission granted.
   * A session expires if user log's out, changes machine, browser or location, if Azkaban is restarted,
   * or if the session expires. The default session timeout is 24 hours (one day). User can re-login irrespective
   * of wheter the session has expired or not. For the same user, a new session will always override the old one.
   * @param username Username.
   * @param password Password.
   * @param azkabanServerUrl Azkaban Server Url.
   * @return Session Id.
   * @throws IOException
   * @throws EncoderException
   */
  public static String authenticateAndGetSessionId(String username, String password, String azkabanServerUrl)
      throws IOException, EncoderException {
    // Create post request
    Map<String, String> params = Maps.newHashMap();
    params.put("action", "login");
    params.put("username", username);
    params.put("password", codec.encode(password));

    return executePostRequest(preparePostRequest(azkabanServerUrl, null, params)).get("session.id");
  }

  /***
   * Get project.id for a Project Name.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @return Project Id.
   * @throws IOException
   */
  public static String getProjectId(String sessionId, AzkabanProjectConfig azkabanProjectConfig) throws IOException {
    // Note: Every get call to Azkaban provides a projectId in response, so we have are using fetchProjectFlows call
    // .. because it does not need any additional params other than project name
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "fetchprojectflows");
    params.put("project", azkabanProjectConfig.getAzkabanProjectName());

    return executeGetRequest(prepareGetRequest(azkabanProjectConfig.getAzkabanServerUrl() + "/manager",
        sessionId, params)).get("projectId");
  }

  /***
   * Creates an Azkaban project and uploads the zip file. If proxy user and group permissions are specified in
   * Azkaban Project Config, then this method also adds it to the project configuration.
   * @param sessionId Session Id.
   * @param zipFilePath Zip file to upload.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @return Project Id.
   * @throws IOException
   */
  public static String createAzkabanProject(String sessionId, String zipFilePath,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "executeFlow");
    params.put("name", azkabanProjectConfig.getAzkabanProjectName());
    params.put("description", azkabanProjectConfig.getAzkabanProjectDescription());

    executePostRequest(preparePostRequest(azkabanProjectConfig.getAzkabanServerUrl() +
        "/manager?action=create", sessionId, params));

    // Add proxy user if any
    if (azkabanProjectConfig.getAzkabanUserToProxy().isPresent()) {
      Iterable<String> proxyUsers = SPLIT_ON_COMMA.split(azkabanProjectConfig.getAzkabanUserToProxy().get());
      for (String user : proxyUsers) {
        addProxyUser(sessionId, azkabanProjectConfig.getAzkabanServerUrl(), azkabanProjectConfig.getAzkabanProjectName(), user);
      }
    }

    // Add group permissions if any
    // TODO: Support users (not just groups), and different permission types
    // (though we can add users, we only support groups at the moment and award them with admin permissions)
    if (StringUtils.isNotBlank(azkabanProjectConfig.getAzkabanGroupAdminUsers())) {
      String [] groups = StringUtils.split(azkabanProjectConfig.getAzkabanGroupAdminUsers(), ",");
      for (String group : groups) {
        addUserPermission(sessionId, azkabanProjectConfig.getAzkabanServerUrl(), azkabanProjectConfig.getAzkabanProjectName(),
            group, true, true, false, false,false,
            false);
      }
    }

    // Upload zip file to azkaban and return projectId
    return uploadZipFileToAzkaban(sessionId, azkabanProjectConfig.getAzkabanServerUrl(), azkabanProjectConfig.getAzkabanProjectName(), zipFilePath);
  }

  /***
   * Deletes an Azkaban project.
   * @param sessionId Session Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @throws IOException
   */
  public static void deleteAzkabanProject(String sessionId, AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    Map<String, String> params = Maps.newHashMap();
    params.put("delete", "true");
    params.put("project", azkabanProjectConfig.getAzkabanProjectName());

    executeGetRequest(prepareGetRequest(azkabanProjectConfig.getAzkabanServerUrl() + "/manager",
        sessionId, params));
  }

  /***
   * Replace an existing Azkaban Project. If proxy user and group permissions are specified in
   * Azkaban Project Config, then this method also adds it to the project configuration.
   * @param sessionId Session Id.
   * @param zipFilePath Zip file to upload.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @return Project Id.
   * @throws IOException
   */
  public static String replaceAzkabanProject(String sessionId, String zipFilePath,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    // Change project description
    changeProjectDescription(sessionId, azkabanProjectConfig.getAzkabanServerUrl(),
        azkabanProjectConfig.getAzkabanProjectName(), azkabanProjectConfig.getAzkabanProjectDescription());

    // Add proxy user if any
    // Note: 1. We cannot remove previous proxy-user because there is no way to read it from Azkaban
    //       2. Adding same proxy user multiple times is a non-issue
    // Add proxy user if any
    if (azkabanProjectConfig.getAzkabanUserToProxy().isPresent()) {
      Iterable<String> proxyUsers = SPLIT_ON_COMMA.split(azkabanProjectConfig.getAzkabanUserToProxy().get());
      for (String user : proxyUsers) {
        addProxyUser(sessionId, azkabanProjectConfig.getAzkabanServerUrl(),
            azkabanProjectConfig.getAzkabanProjectName(), user);
      }
    }

    // Add group permissions if any
    // TODO: Support users (not just groups), and different permission types
    // Note: 1. We cannot remove previous group-user because there is no way to read it from Azkaban
    //       2. Adding same group-user will return an error message, but we will ignore it
    // (though we can add users, we only support groups at the moment and award them with admin permissions)
    if (StringUtils.isNotBlank(azkabanProjectConfig.getAzkabanGroupAdminUsers())) {
      String [] groups = StringUtils.split(azkabanProjectConfig.getAzkabanGroupAdminUsers(), ",");
      for (String group : groups) {
        try {
          addUserPermission(sessionId, azkabanProjectConfig.getAzkabanServerUrl(),
              azkabanProjectConfig.getAzkabanProjectName(), group, true, true,
              false, false, false,false);
        } catch (IOException e) {
          // Ignore if group already exists, we cannot list existing groups; so its okay to attempt adding exiting
          // .. groups
          if (!"Group permission already exists.".equalsIgnoreCase(e.getMessage())) {
            throw e;
          }
        }
      }
    }

    // Upload zip file to azkaban and return projectId
    return uploadZipFileToAzkaban(sessionId, azkabanProjectConfig.getAzkabanServerUrl(),
        azkabanProjectConfig.getAzkabanProjectName(), zipFilePath);
  }

  private static void addProxyUser(String sessionId, String azkabanServerUrl, String azkabanProjectName,
      String proxyUser)
      throws IOException {
    // Create get request (adding same proxy user multiple times is a non-issue, Azkaban handles it)
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "addProxyUser");
    params.put("project", azkabanProjectName);
    params.put("name", proxyUser);

    executeGetRequest(prepareGetRequest(azkabanServerUrl + "/manager", sessionId, params));
  }

  private static void addUserPermission(String sessionId, String azkabanServerUrl, String azkabanProjectName,
      String name, boolean isGroup, boolean adminPermission, boolean readPermission, boolean writePermission,
      boolean executePermission, boolean schedulePermission)
      throws IOException {

    // NOTE: We are not listing the permissions before adding them, because Azkaban in its current state only
    // .. returns user permissions and not group permissions

    // Create get request (adding same normal user permission multiple times will throw an error, but we cannot
    // list whole list of permissions anyways)
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "addPermission");
    params.put("project", azkabanProjectName);
    params.put("name", name);
    params.put("group", Boolean.toString(isGroup));
    params.put("permissions[admin]", Boolean.toString(adminPermission));
    params.put("permissions[read]", Boolean.toString(readPermission));
    params.put("permissions[write]", Boolean.toString(writePermission));
    params.put("permissions[execute]", Boolean.toString(executePermission));
    params.put("permissions[schedule]", Boolean.toString(schedulePermission));

    executeGetRequest(prepareGetRequest(azkabanServerUrl + "/manager", sessionId, params));
  }

  /***
   * Schedule the Azkaban Project to run with a schedule.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @throws IOException
   */
  public static void scheduleAzkabanProject(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "scheduleFlow");
    params.put("projectName", azkabanProjectConfig.getAzkabanProjectName());
    params.put("flow", azkabanProjectConfig.getAzkabanProjectFlowName());
    params.put("projectId", azkabanProjectId);
    params.put("scheduleTime", getScheduledTimeInAzkabanFormat(LOW_NETWORK_TRAFFIC_BEGIN_HOUR,
        LOW_NETWORK_TRAFFIC_END_HOUR, JOB_START_DELAY_MINUTES));
    params.put("scheduleDate", getScheduledDateInAzkabanFormat());
    params.put("is_recurring", "off");

    // Run once OR push down schedule (TODO: Enable when push down is finalized)
    //    if (azkabanProjectConfig.isScheduled()) {
    //      params.put("is_recurring", "on");
    //      params.put("period", "1d");
    //    } else {
    //      params.put("is_recurring", "off");
    //    }

    executePostRequest(preparePostRequest(azkabanProjectConfig.getAzkabanServerUrl() + "/schedule", sessionId, params));
  }

  private static void changeProjectDescription(String sessionId, String azkabanServerUrl, String azkabanProjectName,
      String projectDescription)
      throws IOException {
    String encodedProjectDescription;
    try {
      encodedProjectDescription = new URLCodec().encode(projectDescription);
    } catch (EncoderException e) {
      throw new IOException("Could not encode Azkaban project description", e);
    }

    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "changeDescription");
    params.put("project", azkabanProjectName);
    params.put("description", encodedProjectDescription);

    executeGetRequest(prepareGetRequest(azkabanServerUrl + "/manager", sessionId, params));
  }

  /***
   * Execute an existing Azkaban project.
   * @param sessionId Session Id.
   * @param azkabanProjectId Project Id.
   * @param azkabanProjectConfig Azkaban Project Config.
   * @throws IOException
   */
  public static void executeAzkabanProject(String sessionId, String azkabanProjectId,
      AzkabanProjectConfig azkabanProjectConfig)
      throws IOException {
    Map<String, String> params = Maps.newHashMap();
    params.put("ajax", "executeFlow");
    params.put("project", azkabanProjectConfig.getAzkabanProjectName());
    params.put("flow", azkabanProjectConfig.getAzkabanProjectFlowName());

    executePostRequest(preparePostRequest(azkabanProjectConfig.getAzkabanServerUrl() + "/executor", sessionId, params));
  }

  private static HttpGet prepareGetRequest(String requestUrl, String sessionId, Map<String, String> params)
      throws IOException {
    // Create get request
    StringBuilder stringEntityBuilder = new StringBuilder();
    stringEntityBuilder.append(String.format("?session.id=%s", sessionId));
    for (Map.Entry<String, String> entry : params.entrySet()) {
      stringEntityBuilder.append(String.format("&%s=%s", entry.getKey(), entry.getValue()));
    }

    return new HttpGet(requestUrl + stringEntityBuilder);
  }

  private static HttpPost preparePostRequest(String requestUrl, String sessionId, Map<String, String> params)
      throws IOException {
    // Create post request
    HttpPost postRequest = new HttpPost(requestUrl);
    StringBuilder stringEntityBuilder = new StringBuilder();
    stringEntityBuilder.append(String.format("session.id=%s", sessionId));
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (stringEntityBuilder.length() > 0) {
        stringEntityBuilder.append("&");
      }
      stringEntityBuilder.append(String.format("%s=%s", entry.getKey(), entry.getValue()));
    }
    StringEntity input = new StringEntity(stringEntityBuilder.toString());
    input.setContentType("application/x-www-form-urlencoded");
    postRequest.setEntity(input);
    postRequest.setHeader("X-Requested-With", "XMLHttpRequest");

    return postRequest;
  }

  @VisibleForTesting
  protected static Map<String, String> executeGetRequest(HttpGet getRequest) throws IOException {
    // Make the call, get response
    @Cleanup CloseableHttpClient httpClient = getHttpClient();
    HttpResponse response = httpClient.execute(getRequest);
    return handleResponse(response);
  }

  @VisibleForTesting
  protected static Map<String, String> executePostRequest(HttpPost postRequest) throws IOException {
    // Make the call, get response
    @Cleanup CloseableHttpClient httpClient = getHttpClient();
    HttpResponse response = httpClient.execute(postRequest);
    return handleResponse(response);
  }

  private static String uploadZipFileToAzkaban(String sessionId, String azkabanServerUrl, String azkabanProjectName,
      String jobZipFile)
      throws IOException {

    // Create post request
    HttpPost postRequest = new HttpPost(azkabanServerUrl + "/manager");
    HttpEntity entity = MultipartEntityBuilder
        .create()
        .addTextBody("session.id", sessionId)
        .addTextBody("ajax", "upload")
        .addBinaryBody("file", new File(jobZipFile),
            ContentType.create("application/zip"), azkabanProjectName + ".zip")
        .addTextBody("project", azkabanProjectName)
        .build();
    postRequest.setEntity(entity);

    // Make the call, get response
    @Cleanup CloseableHttpClient httpClient = getHttpClient();
    HttpResponse response = httpClient.execute(postRequest);

    // Obtaining projectId is hard. Uploading zip file is one avenue to get it from Azkaban
    return handleResponse(response, "projectId").get("projectId");
  }

  private static CloseableHttpClient getHttpClient()
      throws IOException {
    try {
      // Self sign SSL
      SSLContextBuilder builder = new SSLContextBuilder();
      builder.loadTrustMaterial(null, (TrustStrategy) new TrustSelfSignedStrategy());
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());

      // Create client
      return HttpClients.custom().setSSLSocketFactory(sslsf).setDefaultCookieStore(new BasicCookieStore()).build();
    } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      throw new IOException("Issue with creating http client", e);
    }
  }

  private static Map<String, String> handleResponse(HttpResponse response, String... responseKeys)
      throws IOException {
    if (response.getStatusLine().getStatusCode() != 201 && response.getStatusLine().getStatusCode()!= 200) {
      log.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
      throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
    }

    // Get response in string
    InputStream in = response.getEntity().getContent();
    String jsonResponseString = IOUtils.toString(in, "UTF-8");
    log.info("Response string: " + jsonResponseString);

    // Parse Json
    Map<String, String> responseMap = new HashMap<>();
    if (StringUtils.isNotBlank(jsonResponseString)) {
      JsonObject jsonObject = new JsonParser().parse(jsonResponseString).getAsJsonObject();

      // Handle error if any
      handleResponseError(jsonObject);

      // Get all responseKeys
      for(Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        responseMap.put(entry.getKey(), entry.getValue().toString().replaceAll("\"", ""));
      }
    }

    return responseMap;
  }

  private static void handleResponseError(JsonObject jsonObject) throws IOException {
    // Azkaban does not has a standard for error messages tag
    if (null != jsonObject.get("status") && "error".equalsIgnoreCase(jsonObject.get("status").toString()
        .replaceAll("\"", ""))) {
      String message = (null != jsonObject.get("message")) ?
          jsonObject.get("message").toString().replaceAll("\"", "") : "Issue in creating project";
      throw new IOException(message);
    }

    if (null != jsonObject.get("error")) {
      String error = jsonObject.get("error").toString().replaceAll("\"", "");
      throw new IOException(error);
    }
  }

  /***
   * Generate a random scheduled time between specified execution time window in the Azkaban compatible format
   * which is: hh,mm,a,z Eg. ScheduleTime=12,00,PM,PDT
   *
   * @param windowStartHour Window start hour in 24 hr (HH) format (inclusive)
   * @param windowEndHour Window end hour in 24 hr (HH) format (exclusive)
   * @param delayMinutes If current time is within window, then additional delay for bootstrapping if desired
   * @return Scheduled time string of the format hh,mm,a,z
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "DMI_RANDOM_USED_ONLY_ONCE",
      justification = "As expected for randomization")
  public static String getScheduledTimeInAzkabanFormat(int windowStartHour, int windowEndHour, int delayMinutes) {
    // Validate
    if (windowStartHour < 0 || windowEndHour > 23 || windowStartHour >= windowEndHour) {
      throw new IllegalArgumentException("Window start should be less than window end, and both should be between "
          + "0 and 23");
    }
    if (delayMinutes < 0 || delayMinutes > 59) {
      throw new IllegalArgumentException("Delay in minutes should be between 0 and 59 (inclusive)");
    }

    // Setup window
    Calendar windowStartTime = Calendar.getInstance();
    windowStartTime.set(Calendar.HOUR_OF_DAY, windowStartHour);
    windowStartTime.set(Calendar.MINUTE, 0);
    windowStartTime.set(Calendar.SECOND, 0);

    Calendar windowEndTime = Calendar.getInstance();
    windowEndTime.set(Calendar.HOUR_OF_DAY, windowEndHour);
    windowEndTime.set(Calendar.MINUTE, 0);
    windowEndTime.set(Calendar.SECOND, 0);

    // Check if current time is between windowStartTime and windowEndTime, then let the execution happen
    // after delayMinutes minutes
    Calendar now = Calendar.getInstance();
    if (now.after(windowStartTime) && now.before(windowEndTime)) {
      // Azkaban takes a few seconds / a minute to bootstrap,
      // so extra few minutes get the first execution to run instantly
      now.add(Calendar.MINUTE, delayMinutes);

      return new SimpleDateFormat("hh,mm,a,z").format(now.getTime());
    }

    // Current time is not between windowStartTime and windowEndTime, so get random execution time for next day
    int allowedSchedulingWindow = (int)((windowEndTime.getTimeInMillis() - windowStartTime.getTimeInMillis()) /
        MILLISECONDS_IN_HOUR);
    int randomHourInWindow = new Random(System.currentTimeMillis()).nextInt(allowedSchedulingWindow);
    int randomMinute = new Random(System.currentTimeMillis()).nextInt(60);
    windowStartTime.add(Calendar.HOUR, randomHourInWindow);
    windowStartTime.set(Calendar.MINUTE, randomMinute);

    return new SimpleDateFormat("hh,mm,a,z").format(windowStartTime.getTime());
  }

  private static String getScheduledDateInAzkabanFormat() {
    // Eg. ScheduleDate=07/22/2014"
    return new SimpleDateFormat("MM/dd/yyyy").format(new Date());
  }
}
