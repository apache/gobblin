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
  protected final Logger log;
  protected final boolean debugLogEnabled;
  protected final CloseableHttpClient client;
  // Mutable state
  private HttpHost curHttpHost = null;
  private long numRecordsWritten = 0;
  private long numBytesWritten = 0;
  private Optional<HttpUriRequest> curRequest = Optional.absent();

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
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(this.getClass());
    this.debugLogEnabled = this.log.isDebugEnabled();
    httpClientInject.setConnectionManager(new HttpClientConnectionManagerWithConnTracking(connManager));
    this.client = httpClientInject.build();
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws IOException {
    this.client.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long recordsWritten() {
    return this.numRecordsWritten;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long bytesWritten() throws IOException {
    return this.numBytesWritten;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record) throws IOException {
    this.curRequest = onNewRecord(record, this.curRequest);
    if (this.curRequest.isPresent()) {
      ListenableFuture<HttpResponse> responseFuture = sendRequest(this.curRequest.get());
      waitForResponse(responseFuture);
      try {
        processResponse(responseFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Logger getLog() {
    return this.log;
  }

  public HttpHost getCurServerHost() {
    if (null == this.curHttpHost) {
      setCurServerHost(chooseServerHost());
    }
    if (null == this.curHttpHost) {
      throw new RuntimeException("No server host selected!");
    }
    return this.curHttpHost;
  }

  /** Clears the current http host so that next request will trigger a new selection using
   * {@link #chooseServerHost() */
  void clearCurServerHost() {
    this.curHttpHost = null;
  }

  void setCurServerHost(HttpHost curHttpHost) {
    this.log.info("Setting current HTTP server host to: " + curHttpHost);
    this.curHttpHost = curHttpHost;
  }

}
