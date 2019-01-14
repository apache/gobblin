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

import org.apache.http.impl.client.CloseableHttpClient;

/**
 * A {@link SessionManager} that implements session refreshing logic
 * used by {@link AzkabanClient}.
 */
public class AzkabanSessionManager implements SessionManager {
  private CloseableHttpClient httpClient;
  private String url;
  private String username;
  private String password;

  public AzkabanSessionManager(CloseableHttpClient httpClient,
                               String url,
                               String username,
                               String password) {
    this.httpClient = httpClient;
    this.username = username;
    this.password = password;
    this.url = url;
  }

  /**
   * Fetch a session id that can be used in the future to communicate with Azkaban server.
   * @return session id
   */
  public String fetchSession() throws AzkabanClientException {
    return SessionHelper.getSessionId(this.httpClient, this.url, this.username, this.password);
  }
}
