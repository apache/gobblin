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
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Defines the main extension points for the {@link AbstractHttpWriter}.
 * @param D     the type of the data records
 */
public interface HttpWriterDecoration<D> {

  /** An extension point to select the HTTP server to connect to. */
  URI chooseServerHost();

  /**
   * A callback triggered before attempting to connect to a new host. Subclasses can override this
   * method to customize the connect logic.
   * For example, they can implement OAuth authentication.*/
  void onConnect(URI serverHost) throws IOException;

  /**
   * A callback that allows the subclasses to customize the construction of an HTTP request based on
   * incoming records. Customization may include, setting the URL, headers, buffering, etc.
   *
   * @param record      the new record to be written
   * @param request     the current request object; if absent the implementation is responsible of
   *                    allocating a new object
   * @return the current request object; if absent no further processing will happen
   *
   */
  Optional<HttpUriRequest> onNewRecord(D record);

  /**
   * An extension point to send the actual request to the remote server.
   * @param  request         the request to be sent
   * @return a future that allows access to the response. Response may be retrieved synchronously or
   *         asynchronously.
   */
  ListenableFuture<CloseableHttpResponse> sendRequest(HttpUriRequest request) throws IOException ;

  /**
   * Customize the waiting for an HTTP response. Can add timeout logic.
   * @param responseFuture  the future object of the last sent request
   */
  CloseableHttpResponse waitForResponse(ListenableFuture<CloseableHttpResponse> responseFuture);

  /**
   * Processes the response
   * @param response
   * @throws  IOException if there was a problem reading the response
   * @throws  UnexpectedResponseException if the response was unexpected
   */
  void processResponse(CloseableHttpResponse response) throws IOException, UnexpectedResponseException;

}
