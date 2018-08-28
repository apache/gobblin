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

import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * Builder for {@link AzkabanClient} instances.
 *
 * User has to provide below attributes:
 * {@link AzkabanClientBuilder#url}
 * {@link AzkabanClientBuilder#username}
 * {@link AzkabanClientBuilder#password}
 *
 * A customized builder can be provided by extending this class.
 */
@Getter
public class AzkabanClientBuilder {

  private String url = null;
  private String username = null;
  private String password = null;
  private long sessionExpireInMin = 12 * 60; // default is 12h

  protected AzkabanClientBuilder() {
    super();
  }

  public static AzkabanClientBuilder create() {
    return new AzkabanClientBuilder();
  }

  public final AzkabanClientBuilder setUserName(String username) {
    this.username = username;
    return this;
  }

  public final AzkabanClientBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public final AzkabanClientBuilder setUrl (String url) {
    this.url = url;
    return this;
  }

  public final AzkabanClientBuilder setSessionExpireInMin(long minutes) {
    this.sessionExpireInMin = minutes;
    return this;
  }

  public AzkabanClient build() throws AzkabanClientException {
    Preconditions.checkArgument(url != null);
    Preconditions.checkArgument(username != null);
    Preconditions.checkArgument(password != null);
    return new AzkabanClient(this);
  }
}
