/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
