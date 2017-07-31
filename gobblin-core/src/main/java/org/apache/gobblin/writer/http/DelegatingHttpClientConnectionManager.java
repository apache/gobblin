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
package org.apache.gobblin.writer.http;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpClientConnection;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.protocol.HttpContext;


/**
 * Helper class to decorate HttpClientConnectionManager instances.
 */
public class DelegatingHttpClientConnectionManager implements HttpClientConnectionManager {
  protected final HttpClientConnectionManager fallbackConnManager;

  public DelegatingHttpClientConnectionManager(HttpClientConnectionManager fallback) {
    this.fallbackConnManager = fallback;
  }

  @Override
  public void releaseConnection(HttpClientConnection conn, Object newState, long validDuration, TimeUnit timeUnit) {
    this.fallbackConnManager.releaseConnection(conn, newState, validDuration, timeUnit);
  }

  @Override
  public void connect(HttpClientConnection conn, HttpRoute route, int connectTimeout, HttpContext context)
      throws IOException {
    this.fallbackConnManager.connect(conn, route, connectTimeout, context);
  }

  @Override
  public void upgrade(HttpClientConnection conn, HttpRoute route, HttpContext context) throws IOException {
    this.fallbackConnManager.upgrade(conn, route, context);
  }

  @Override
  public void routeComplete(HttpClientConnection conn, HttpRoute route, HttpContext context) throws IOException {
    this.fallbackConnManager.routeComplete(conn, route, context);
  }

  @Override
  public void closeIdleConnections(long idletime, TimeUnit tunit) {
    this.fallbackConnManager.closeIdleConnections(idletime, tunit);
  }

  @Override
  public void closeExpiredConnections() {
    this.fallbackConnManager.closeExpiredConnections();
  }

  @Override
  public void shutdown() {
    this.fallbackConnManager.shutdown();
  }

  @Override
  public ConnectionRequest requestConnection(HttpRoute route, Object state) {
    return this.fallbackConnManager.requestConnection(route, state);
  }

}
