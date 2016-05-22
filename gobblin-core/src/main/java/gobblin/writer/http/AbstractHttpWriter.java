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
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import gobblin.configuration.State;
import gobblin.instrumented.writer.InstrumentedDataWriter;


/**
 * Base class for HTTP writers. Defines the main extension points for different implementations.
 */
public abstract class AbstractHttpWriter<D> extends InstrumentedDataWriter<D> implements HttpWriterDecoration<D> {

  // Immutable state
  protected final Logger _log;
  protected final boolean _debugLogEnabled;
  protected final CloseableHttpClient _client;
  // Mutable state
  private HttpHost _curHttpHost = null;
  private long _numRecordsWritten = 0;
  private long _numBytesWritten = 0;
  private Optional<HttpUriRequest> _curRequest = Optional.absent();

  class HttpClientConnectionManagerWithConnTracking extends DelegatingHttpClientConnectionManager {

    public HttpClientConnectionManagerWithConnTracking(HttpClientConnectionManager fallback) {
      super(fallback);
    }

    @Override
    public ConnectionRequest requestConnection(HttpRoute route, Object state) {
      try {
        onConnect(route.getTargetHost());
      } catch (IOException e) {
        throw new RuntimeException("onConnect() callback failure: " + e, e);
      }
      return super.requestConnection(route, state);
    }

  }

  public AbstractHttpWriter(State state, Optional<Logger> log, HttpClientBuilder httpClientInject,
      HttpClientConnectionManager connManager) {
    super(state);
    this._log = log.isPresent() ? log.get() : LoggerFactory.getLogger(this.getClass());
    this._debugLogEnabled = this._log.isDebugEnabled();
    httpClientInject.setConnectionManager(new HttpClientConnectionManagerWithConnTracking(connManager));
    this._client = httpClientInject.build();
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws IOException {
    this._client.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long recordsWritten() {
    return this._numRecordsWritten;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long bytesWritten() throws IOException {
    return this._numBytesWritten;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record) throws IOException {
    this._curRequest = onNewRecord(record, this._curRequest);
    if (this._curRequest.isPresent()) {
      ListenableFuture<HttpResponse> responseFuture = sendRequest(this._curRequest.get());
      waitForResponse(responseFuture);
      try {
        processResponse(responseFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Logger getLog() {
    return this._log;
  }

  public HttpHost getCurServerHost() {
    if (null == this._curHttpHost) {
      setCurServerHost(chooseServerHost());
    }
    if (null == this._curHttpHost) {
      throw new RuntimeException("No server host selected!");
    }
    return this._curHttpHost;
  }

  /** Clears the current http host so that next request will trigger a new selection using
   * {@link #chooseServerHost() */
  void clearCurServerHost() {
    this._curHttpHost = null;
  }

  void setCurServerHost(HttpHost curHttpHost) {
    this._log.info("Setting current HTTP server host to: " + curHttpHost);
    this._curHttpHost = curHttpHost;
  }

}
