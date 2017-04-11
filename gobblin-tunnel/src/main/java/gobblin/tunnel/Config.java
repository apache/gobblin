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

package gobblin.tunnel;

class Config {
  public static final int PROXY_CONNECT_TIMEOUT_MS = 5000;
  private final String remoteHost;
  private final int remotePort;
  private final String proxyHost;
  private final int proxyPort;

  public Config(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
  }

  public String getRemoteHost() {
    return this.remoteHost;
  }

  public int getRemotePort() {
    return this.remotePort;
  }

  public String getProxyHost() {
    return this.proxyHost;
  }

  public int getProxyPort() {
    return this.proxyPort;
  }
}
