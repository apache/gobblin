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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Common parent for {@link AbstractHttpWriter} decorators. Delegates extension methods to another
 * implementation and simplifies the overriding of only selected methods
 */
public abstract class HttpWriterDecorator<D> implements HttpWriterDecoration<D> {

  private final HttpWriterDecoration<D> fallback;

  public HttpWriterDecorator(HttpWriterDecoration<D> fallback) {
    Preconditions.checkNotNull(fallback);
    this.fallback = fallback;
  }

  protected HttpWriterDecoration<D> getFallback() {
    return this.fallback;
  }

  @Override
  public URI chooseServerHost() {
    return getFallback().chooseServerHost();
  }

  @Override
  public void onConnect(URI serverHost) throws IOException {
    getFallback().onConnect(serverHost);
  }

  @Override
  public Optional<HttpUriRequest> onNewRecord(D record) {
    return getFallback().onNewRecord(record);
  }

  @Override
  public ListenableFuture<CloseableHttpResponse> sendRequest(HttpUriRequest request) throws IOException {
    return getFallback().sendRequest(request);
  }

  @Override
  public CloseableHttpResponse waitForResponse(ListenableFuture<CloseableHttpResponse> responseFuture) {
    return getFallback().waitForResponse(responseFuture);
  }

  @Override
  public void processResponse(CloseableHttpResponse response) throws IOException, UnexpectedResponseException {
    getFallback().processResponse(response);
  }

}
