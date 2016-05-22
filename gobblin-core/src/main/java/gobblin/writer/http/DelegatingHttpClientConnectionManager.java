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
package gobblin.writer.http;

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
  protected final HttpClientConnectionManager _fallbackConnManager;

  public DelegatingHttpClientConnectionManager(HttpClientConnectionManager fallback) {
    this._fallbackConnManager = fallback;
  }

  @Override
  public void releaseConnection(HttpClientConnection conn, Object newState, long validDuration, TimeUnit timeUnit) {
    this._fallbackConnManager.releaseConnection(conn, newState, validDuration, timeUnit);
  }

  @Override
  public void connect(HttpClientConnection conn, HttpRoute route, int connectTimeout, HttpContext context)
      throws IOException {
    this._fallbackConnManager.connect(conn, route, connectTimeout, context);
  }

  @Override
  public void upgrade(HttpClientConnection conn, HttpRoute route, HttpContext context) throws IOException {
    this._fallbackConnManager.upgrade(conn, route, context);
  }

  @Override
  public void routeComplete(HttpClientConnection conn, HttpRoute route, HttpContext context) throws IOException {
    this._fallbackConnManager.routeComplete(conn, route, context);
  }

  @Override
  public void closeIdleConnections(long idletime, TimeUnit tunit) {
    this._fallbackConnManager.closeIdleConnections(idletime, tunit);
  }

  @Override
  public void closeExpiredConnections() {
    this._fallbackConnManager.closeExpiredConnections();
  }

  @Override
  public void shutdown() {
    this._fallbackConnManager.shutdown();
  }

  @Override
  public ConnectionRequest requestConnection(HttpRoute route, Object state) {
    return this._fallbackConnManager.requestConnection(route, state);
  }

}
