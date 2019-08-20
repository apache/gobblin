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

package org.apache.gobblin.salesforce;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicNameValuePair;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.source.extractor.exception.RestApiConnectionException;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiConnector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * An extension of {@link RestApiConnector} for Salesforce API.
 */
@Slf4j
public class SalesforceConnector extends RestApiConnector {

  private static final String DEFAULT_SERVICES_DATA_PATH = "/services/data";
  private static final String DEFAULT_AUTH_TOKEN_PATH = "/services/oauth2/token";
  protected String refreshToken;

  public SalesforceConnector(State state) {
    super(state);
    if (isPasswordGrant(state)) {
      this.refreshToken = null;
    } else {
      this.refreshToken = state.getProp(ConfigurationKeys.SOURCE_CONN_REFRESH_TOKEN);
    }
  }

  @Getter
  private String servicesDataEnvPath;

  @Override
  public HttpEntity getAuthentication() throws RestApiConnectionException {
    log.debug("Authenticating salesforce");
    String clientId = this.state.getProp(ConfigurationKeys.SOURCE_CONN_CLIENT_ID);
    String clientSecret = this.state.getProp(ConfigurationKeys.SOURCE_CONN_CLIENT_SECRET);
    if (this.state.getPropAsBoolean(ConfigurationKeys.SOURCE_CONN_DECRYPT_CLIENT_SECRET, false)) {
      PasswordManager passwordManager = PasswordManager.getInstance(this.state);
      clientId = passwordManager.readPassword(clientId);
      clientSecret = passwordManager.readPassword(clientSecret);
    }
    String host = this.state.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);

    List<NameValuePair> formParams = Lists.newArrayList();
    formParams.add(new BasicNameValuePair("client_id", clientId));
    formParams.add(new BasicNameValuePair("client_secret", clientSecret));

    if (refreshToken == null) {
      log.info("Authenticating salesforce with username/password");
      String userName = this.state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
      String password = PasswordManager.getInstance(this.state)
          .readPassword(this.state.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
      String securityToken = PasswordManager.getInstance(this.state)
          .readPassword(this.state.getProp(ConfigurationKeys.SOURCE_CONN_SECURITY_TOKEN));
      formParams.add(new BasicNameValuePair("grant_type", "password"));
      formParams.add(new BasicNameValuePair("username", userName));
      formParams.add(new BasicNameValuePair("password", password + securityToken));
    } else {
      log.info("Authenticating salesforce with refresh_token");
      formParams.add(new BasicNameValuePair("grant_type", "refresh_token"));
      formParams.add(new BasicNameValuePair("refresh_token", refreshToken));
    }
    try {
      HttpPost post = new HttpPost(host + DEFAULT_AUTH_TOKEN_PATH);
      post.setEntity(new UrlEncodedFormEntity(formParams));

      HttpResponse httpResponse = getHttpClient().execute(post);
      HttpEntity httpEntity = httpResponse.getEntity();

      return httpEntity;
    } catch (Exception e) {
      throw new RestApiConnectionException("Failed to authenticate salesforce host:"
          + host + "; error-" + e.getMessage(), e);
    }
  }

  @Override
  protected void addHeaders(HttpRequestBase httpRequest) {
    if (refreshToken == null) {
      super.addHeaders(httpRequest);
    } else {
      if (this.accessToken != null) {
        httpRequest.addHeader("Authorization", "Bearer " + this.accessToken);
      }
      httpRequest.addHeader("Content-Type", "application/json");
    }
  }

  static boolean isPasswordGrant(State state) {
    String userName = state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
    String securityToken = state.getProp(ConfigurationKeys.SOURCE_CONN_SECURITY_TOKEN);
    return (userName != null &&  securityToken != null);
  }

  private String getServiceBaseUrl() {
    String dataEnvPath = DEFAULT_SERVICES_DATA_PATH + "/v" + this.state.getProp(ConfigurationKeys.SOURCE_CONN_VERSION);
    this.servicesDataEnvPath = dataEnvPath;
    return this.instanceUrl + dataEnvPath;
  }

  public String getFullUri(String resourcePath) {
    return StringUtils.removeEnd(getServiceBaseUrl(), "/") + StringUtils.removeEnd(resourcePath, "/");
  }

  String getAccessToken() {
    return accessToken;
  }

  String getInstanceUrl() {
    return instanceUrl;
  }
}
