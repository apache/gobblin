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

import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A helper class which can get session id using Azkaban authentication mechanism.
 *
 * @see <a href="https://azkaban.github.io/azkaban/docs/latest/#api-authenticate">
 *   https://azkaban.github.io/azkaban/docs/latest/#api-authenticate
 *  </a>
 */
public class SessionHelper {

  /**
   * <p>Use Azkaban ajax api to fetch the session id. Required http request parameters are:
   *   <br>action=login	The fixed parameter indicating the login action.
   *   <br>username	The Azkaban username.
   *   <br>password	The corresponding password.
   * </pr>
   *
   * @param httpClient An apache http client
   * @param url Azkaban ajax endpoint
   * @param username username for Azkaban login
   * @param password password for Azkaban login
   *
   * @return session id
   */
  public static String getSessionId(CloseableHttpClient httpClient, String url, String username, String password)
      throws AzkabanClientException {
    try {
      HttpPost httpPost = new HttpPost(url);
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair(AzkabanClientParams.ACTION, "login"));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.USERNAME, username));
      nvps.add(new BasicNameValuePair(AzkabanClientParams.PASSWORD, password));
      httpPost.setEntity(new UrlEncodedFormEntity(nvps));
      CloseableHttpResponse response = httpClient.execute(httpPost);

      try {
        HttpEntity entity = response.getEntity();

        // retrieve session id from entity
        String jsonResponseString = IOUtils.toString(entity.getContent(), "UTF-8");
        JsonObject jsonObject = AzkabanClient.parseResponse(jsonResponseString);
        Map<String, String> responseMap = AzkabanClient.getFlatMap(jsonObject);
        String sessionId = responseMap.get(AzkabanClientParams.SESSION_ID);
        EntityUtils.consume(entity);
        return sessionId;
      } catch (Exception e) {
        throw new AzkabanClientException("Azkaban client cannot consume session response.", e);
      } finally {
        response.close();
      }
    } catch (Exception e) {
      throw new AzkabanClientException("Azkaban client cannot fetch session.", e);
    }
  }
}
