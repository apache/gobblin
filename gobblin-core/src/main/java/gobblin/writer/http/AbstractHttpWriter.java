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
package gobblin.writer.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import gobblin.instrumented.writer.InstrumentedDataWriter;

/**
 * Base class for HTTP writers. Defines the main extension points for different implementations.
 */
public abstract class AbstractHttpWriter<D> extends InstrumentedDataWriter<D> implements HttpWriterDecoration<D> {

  // Immutable state
  protected final Logger log;
  protected final boolean debugLogEnabled;
  protected final CloseableHttpClient client;
  private final ListeningExecutorService singleThreadPool;

  // Mutable state
  private URI curHttpHost = null;
  private long numRecordsWritten = 0L;
  private long numBytesWritten = 0L; //AbstractHttpWriter won't update as it could be expensive.
  Optional<HttpUriRequest> curRequest = Optional.absent();

  class HttpClientConnectionManagerWithConnTracking extends DelegatingHttpClientConnectionManager {

    public HttpClientConnectionManagerWithConnTracking(HttpClientConnectionManager fallback) {
      super(fallback);
    }

    @Override
    public ConnectionRequest requestConnection(HttpRoute route, Object state) {
      try {
        onConnect(new URI(route.getTargetHost().toURI()));
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException("onConnect() callback failure: " + e, e);
      }
      return super.requestConnection(route, state);
    }

  }

  @SuppressWarnings("rawtypes")
  public AbstractHttpWriter(AbstractHttpWriterBuilder builder) {
    super(builder.getState());
    this.log = builder.getLogger().isPresent() ? (Logger)builder.getLogger() : LoggerFactory.getLogger(this.getClass());
    this.debugLogEnabled = this.log.isDebugEnabled();

    HttpClientBuilder httpClientBuilder = builder.getHttpClientBuilder();
    httpClientBuilder.setConnectionManager(new HttpClientConnectionManagerWithConnTracking(builder.getHttpConnManager()));
    this.client = httpClientBuilder.build();
    this.singleThreadPool = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    if (builder.getSvcEndpoint().isPresent()) {
      setCurServerHost((URI) builder.getSvcEndpoint().get());
    }
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
   * Send and process the request. If it's a retry request, skip onNewRecord method call and go straight sending request.
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record) throws IOException {
    if (!isRetry()) {
      //If currentRequest is still here, it means this is retry request.
      //In this case, don't invoke onNewRecord again as onNewRecord is not guaranteed to be idempotent.
      //(e.g: If you do batch processing duplicate record can go in, etc.)
      curRequest = onNewRecord(record);
    }

    if (curRequest.isPresent()) {
      ListenableFuture<CloseableHttpResponse> responseFuture = sendRequest(curRequest.get());
      try (CloseableHttpResponse response = waitForResponse(responseFuture)) {
        processResponse(response);
      }
      curRequest = Optional.absent(); //Clear request if successful
    }
    numRecordsWritten++;
  }

  /**
   * Prior to commit, it will invoke flush method to flush any remaining item if writer uses batch
   * {@inheritDoc}
   * @see gobblin.instrumented.writer.InstrumentedDataWriterBase#commit()
   */
  @Override
  public void commit() throws IOException {
    flush();
    super.commit();
  }

  /**
   * If writer supports batch, override this method.
   * (Be aware of failure and retry as flush can be called multiple times in case of failure @see SalesforceRestWriter )
   */
  public void flush() { }

  /**
   * Sends request using single thread pool so that it can be easily terminated(use case: time out)
   * {@inheritDoc}
   * @see gobblin.writer.http.HttpWriterDecoration#sendRequest(org.apache.http.client.methods.HttpUriRequest)
   */
  @Override
  public ListenableFuture<CloseableHttpResponse> sendRequest(final HttpUriRequest request) throws IOException {
    return singleThreadPool.submit(new Callable<CloseableHttpResponse>() {
      @Override
      public CloseableHttpResponse call() throws Exception {
        return client.execute(request);
      }
    });
  }

  /**
   * Checks if it's retry request.
   * All successful request should make currentRequest absent. If currentRequest still exists, it means there was a failure.
   * There's couple of methods need this indicator such as onNewRecord, since it is not a new record.
   * @return true if current request it holds is retry.
   */
  public boolean isRetry() {
    return curRequest.isPresent();
  }

  /**
   * Default implementation is to use HttpClients socket timeout which is waiting based on elapsed time between
   * last packet sent from client till receive it from server.
   *
   * {@inheritDoc}
   * @see gobblin.writer.http.HttpWriterDecoration#waitForResponse(com.google.common.util.concurrent.ListenableFuture)
   */
  @Override
  public CloseableHttpResponse waitForResponse(ListenableFuture<CloseableHttpResponse> responseFuture) {
    try {
      return responseFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Default implementation where any status code equal to or greater than 400 is regarded as a failure.
   * {@inheritDoc}
   * @see gobblin.writer.http.HttpWriterDecoration#processResponse(org.apache.http.HttpResponse)
   */
  @Override
  public void processResponse(CloseableHttpResponse response) throws IOException, UnexpectedResponseException {
    if (response.getStatusLine().getStatusCode() >= 400) {
      if (response.getEntity() != null) {
        throw new RuntimeException("Failed. " + EntityUtils.toString(response.getEntity())
                                 + " , response: " + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));
      }
      throw new RuntimeException("Failed. Response: " + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));
    }
  }

  public Logger getLog() {
    return this.log;
  }

  public URI getCurServerHost() {
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

  void setCurServerHost(URI curHttpHost) {
    this.log.info("Setting current HTTP server host to: " + curHttpHost);
    this.curHttpHost = curHttpHost;
  }

}
