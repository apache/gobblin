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

package gobblin.writer;

import java.io.IOException;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequest;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;
import gobblin.async.DispatchException;
import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;


/**
 * This class is an {@link AsyncHttpWriter} that writes data in a batch, which
 * is sent via http request
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
@Slf4j
public class AsyncHttpWriter<D, RQ, RP> extends AbstractAsyncDataWriter<D> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHttpWriter.class);

  public static final int DEFAULT_MAX_ATTEMPTS = 3;

  private final HttpClient<RQ, RP> httpClient;
  private final ResponseHandler<RQ, RP> responseHandler;
  private final AsyncRequestBuilder<D, RQ> requestBuilder;
  private final int maxAttempts;

  public AsyncHttpWriter(AsyncHttpWriterBuilder builder) {
    super(builder.getQueueCapacity());
    this.httpClient = builder.getClient();
    this.requestBuilder = builder.getAsyncRequestBuilder();
    this.responseHandler = builder.getResponseHandler();
    this.maxAttempts = builder.getMaxAttempts();
  }

  @Override
  protected void dispatch(Queue<BufferedRecord<D>> buffer) throws DispatchException {
    AsyncRequest<D, RQ> asyncRequest = requestBuilder.buildRequest(buffer);
    if (asyncRequest == null) {
      return;
    }

    RQ rawRequest = asyncRequest.getRawRequest();
    RP response;

    int attempt = 0;
    while (attempt < maxAttempts) {
      try {
          response = httpClient.sendRequest(rawRequest);
      } catch (IOException e) {
        // Retry
        attempt++;
        if (attempt == maxAttempts) {
          LOG.error("Fail to send request");
          LOG.info(asyncRequest.toString());

          onFailure(asyncRequest, e);
          throw new DispatchException("Write failed on IOException", e);
        } else {
          continue;
        }
      }

      ResponseStatus status = responseHandler.handleResponse(asyncRequest, response);
      switch (status.getType()) {
        case OK:
          // Write succeeds
          onSuccess(asyncRequest, status);
          return;
        case CONTINUE:
          LOG.debug("Http write continues");
          LOG.debug(asyncRequest.toString());

          onSuccess(asyncRequest, status);
          return;
        case CLIENT_ERROR:
          // Client error. Fail!
          LOG.error("Http write failed on client error");
          LOG.info(asyncRequest.toString());

          DispatchException clientExp = new DispatchException("Write failed on client error");
          onFailure(asyncRequest, clientExp);
          throw clientExp;
        case SERVER_ERROR:
          // Server side error. Retry
          attempt++;
          if (attempt == maxAttempts) {
            LOG.error("Http write request failed on server error");
            LOG.info(asyncRequest.toString());

            DispatchException serverExp = new DispatchException("Write failed after " + maxAttempts + " attempts.");
            onFailure(asyncRequest, serverExp);
            throw serverExp;
          }
      }
    }
  }

  /**
   * Callback on sending the asyncRequest successfully
   */
  protected void onSuccess(AsyncRequest<D, RQ> asyncRequest, ResponseStatus status) {
    final WriteResponse response = WriteResponse.EMPTY;
    for (final AsyncRequest.Thunk thunk: asyncRequest.getThunks()) {
      WriteCallback callback = (WriteCallback) thunk.callback;
      callback.onSuccess(new WriteResponse() {
        @Override
        public Object getRawResponse() {
          return response.getRawResponse();
        }

        @Override
        public String getStringResponse() {
          return response.getStringResponse();
        }

        @Override
        public long bytesWritten() {
          return thunk.sizeInBytes;
        }
      });
    }
  }

  /**
   * Callback on failing to send the asyncRequest
   */
  protected void onFailure(AsyncRequest<D, RQ> asyncRequest, Throwable throwable) {
    for (AsyncRequest.Thunk thunk: asyncRequest.getThunks()) {
      thunk.callback.onFailure(throwable);
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      super.close();
    } finally {
      httpClient.close();
    }
  }
}
