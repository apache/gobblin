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

package org.apache.gobblin.source.extractor.extract.restapi;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.http.HttpClientConfiguratorLoader;
import org.apache.gobblin.source.extractor.exception.RestApiConnectionException;
import org.apache.gobblin.source.extractor.exception.RestApiProcessingException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class for connecting to Rest APIs, construct queries and getting responses.
 */
@Slf4j
public abstract class RestApiConnector {

  public static final String REST_API_CONNECTOR_CLASS = "rest.api.connector.class";

  protected static final Gson GSON = new Gson();

  protected HttpClient httpClient = null;
  protected boolean autoEstablishAuthToken = false;

  @Setter
  protected long authTokenTimeout;
  protected String accessToken = null;
  protected long createdAt;
  protected String instanceUrl;
  protected String updatedQuery;

  protected final State state;

  public RestApiConnector(State state) {
    this.state = state;
    this.authTokenTimeout =
        state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_TIMEOUT, ConfigurationKeys.DEFAULT_CONN_TIMEOUT);
  }

  /**
   * get http connection
   * @return true if the connection is success else false
   */
  public boolean connect() throws RestApiConnectionException {
    if (this.autoEstablishAuthToken) {
      if (this.authTokenTimeout <= 0) {
        return false;
      } else if ((System.currentTimeMillis() - this.createdAt) > this.authTokenTimeout) {
        return false;
      }
    }

    HttpEntity httpEntity = null;
    try {
      httpEntity = getAuthentication();

      if (httpEntity != null) {
        JsonElement json = GSON.fromJson(EntityUtils.toString(httpEntity), JsonObject.class);
        if (json == null) {
          log.error("Http entity: " + httpEntity);
          log.error("entity class: " + httpEntity.getClass().getName());
          log.error("entity string size: " + EntityUtils.toString(httpEntity).length());
          log.error("content length: " + httpEntity.getContentLength());
          log.error("content: " + IOUtils.toString(httpEntity.getContent(), Charsets.UTF_8));
          throw new RestApiConnectionException(
              "JSON is NULL ! Failed on authentication with the following HTTP response received:\n"
                  + EntityUtils.toString(httpEntity));
        }

        JsonObject jsonRet = json.getAsJsonObject();
        log.info("jsonRet: " + jsonRet.toString());
        parseAuthenticationResponse(jsonRet);
      }
    } catch (IOException e) {
      throw new RestApiConnectionException("Failed to get rest api connection; error - " + e.getMessage(), e);
    } finally {
      if (httpEntity != null) {
        try {
          EntityUtils.consume(httpEntity);
        } catch (IOException e) {
          throw new RestApiConnectionException("Failed to consume httpEntity; error - " + e.getMessage(), e);
        }
      }
    }

    return true;
  }

  protected HttpClient getHttpClient() {
    if (this.httpClient == null) {
      HttpClientConfiguratorLoader configuratorLoader = new HttpClientConfiguratorLoader(this.state);
      this.httpClient = configuratorLoader.getConfigurator()
          .setStatePropertiesPrefix(ConfigurationKeys.SOURCE_CONN_PREFIX)
          .configure(this.state)
          .createClient();
    }
    return this.httpClient;
  }

  private static boolean hasId(JsonObject json) {
    if (json.has("id") || json.has("Id") || json.has("ID") || json.has("iD")) {
      return true;
    }
    return false;
  }

  /**
   * get http response in json format using url
   * @return json string with the response
   */
  public CommandOutput<?, ?> getResponse(List<Command> cmds) throws RestApiProcessingException {
    String url = cmds.get(0).getParams().get(0);

    log.info("URL: " + url);
    String jsonStr = null;
    HttpRequestBase httpRequest = new HttpGet(url);
    addHeaders(httpRequest);
    HttpEntity httpEntity = null;
    HttpResponse httpResponse = null;
    try {
      httpResponse = this.httpClient.execute(httpRequest);
      StatusLine status = httpResponse.getStatusLine();
      httpEntity = httpResponse.getEntity();

      if (httpEntity != null) {
        jsonStr = EntityUtils.toString(httpEntity);
      }

      if (status.getStatusCode() >= 400) {
        log.info("Unable to get response using: " + url);
        JsonElement jsonRet = GSON.fromJson(jsonStr, JsonArray.class);
        throw new RestApiProcessingException(getFirstErrorMessage("Failed to retrieve response from", jsonRet));
      }
    } catch (Exception e) {
      throw new RestApiProcessingException("Failed to process rest api request; error - " + e.getMessage(), e);
    } finally {
      try {
        if (httpEntity != null) {
          EntityUtils.consume(httpEntity);
        }
        // httpResponse.close();
      } catch (Exception e) {
        throw new RestApiProcessingException("Failed to consume httpEntity; error - " + e.getMessage(), e);
      }
    }
    CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
    output.put((RestApiCommand) cmds.get(0), jsonStr);
    return output;
  }

  protected void addHeaders(HttpRequestBase httpRequest) {
    if (this.accessToken != null) {
      httpRequest.addHeader("Authorization", "OAuth " + this.accessToken);
    }
    httpRequest.addHeader("Content-Type", "application/json");
    //httpRequest.addHeader("Accept-Encoding", "zip");
    //httpRequest.addHeader("Content-Encoding", "gzip");
    //httpRequest.addHeader("Connection", "Keep-Alive");
    //httpRequest.addHeader("Keep-Alive", "timeout=60000");
  }

  /**
   * get error message while executing http url
   * @return error message
   */
  private static String getFirstErrorMessage(String defaultMessage, JsonElement json) {
    if (json == null) {
      return defaultMessage;
    }

    JsonObject jsonObject = null;

    if (!json.isJsonArray()) {
      jsonObject = json.getAsJsonObject();
    } else {
      JsonArray jsonArray = json.getAsJsonArray();
      if (jsonArray.size() != 0) {
        jsonObject = jsonArray.get(0).getAsJsonObject();
      }
    }

    if (jsonObject != null) {
      if (jsonObject.has("error_description")) {
        defaultMessage = defaultMessage + jsonObject.get("error_description").getAsString();
      } else if (jsonObject.has("message")) {
        defaultMessage = defaultMessage + jsonObject.get("message").getAsString();
      }
    }

    return defaultMessage;
  }

  /**
   * Build a list of {@link Command}s given a String Rest query.
   */
  public static List<Command> constructGetCommand(String restQuery) {
    return Arrays.asList(new RestApiCommand().build(Arrays.asList(restQuery), RestApiCommandType.GET));
  }

  public boolean isConnectionClosed() {
    return this.httpClient == null;
  }

  /**
   * To be overridden by subclasses that require authentication.
   */
  public abstract HttpEntity getAuthentication() throws RestApiConnectionException;

  protected void parseAuthenticationResponse(JsonObject jsonRet) throws RestApiConnectionException {
    if (!hasId(jsonRet)) {
      throw new RestApiConnectionException("Failed on authentication with the following HTTP response received:"
          + jsonRet.toString());
    }

    this.instanceUrl = jsonRet.get("instance_url").getAsString();
    this.accessToken = jsonRet.get("access_token").getAsString();
    this.createdAt = System.currentTimeMillis();
  }
}
