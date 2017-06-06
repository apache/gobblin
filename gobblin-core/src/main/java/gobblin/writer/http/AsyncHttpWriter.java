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
import java.util.Queue;

import com.codahale.metrics.Timer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import gobblin.async.DispatchException;
import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.writer.WriteResponse;


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
  public static final int DEFAULT_MAX_ATTEMPTS = 3;
  public static final String DISPATCH_TIMER = "dispatch.timer";

  private final HttpClient<RQ, RP> httpClient;
  private final ResponseHandler<RP> responseHandler;
  private final AsyncWriteRequestBuilder<D, RQ> requestBuilder;
  private final int maxAttempts;
  private final MetricContext metricContext;

  @Getter
  private final Timer dispatchTimer;

  public AsyncHttpWriter(AsyncHttpWriterBuilder builder) {
    super(builder.getQueueCapacity());
    this.httpClient = builder.getClient();
    this.requestBuilder = builder.getAsyncRequestBuilder();
    this.responseHandler = builder.getResponseHandler();
    this.maxAttempts = builder.getMaxAttempts();
    this.metricContext = Instrumented.getMetricContext(builder.getState(), AsyncHttpWriter.class);
    this.dispatchTimer = this.metricContext.timer(DISPATCH_TIMER);
  }

  @Override
  protected void dispatch(Queue<BufferedRecord<D>> buffer) throws DispatchException {
    AsyncWriteRequest<D, RQ> asyncWriteRequest = requestBuilder.buildWriteRequest(buffer);
    if (asyncWriteRequest == null) {
      return;
    }

    RQ rawRequest = asyncWriteRequest.getRawRequest();
    RP response;

    int attempt = 0;
    while (attempt < maxAttempts) {
      final Timer.Context context = dispatchTimer.time();
      try {
          response = httpClient.sendRequest(rawRequest);
      } catch (IOException e) {
        // Retry
        attempt++;
        if (attempt == maxAttempts) {
          asyncWriteRequest.onFailure(e);
          throw new DispatchException("Write failed on IOException", e);
        } else {
          continue;
        }
      } finally {
        context.stop();
      }

      ResponseStatus status = responseHandler.handleResponse(response);
      switch (status.getType()) {
        case OK:
          // Write succeeds
          asyncWriteRequest.onSuccess(WriteResponse.EMPTY);
          return;
        case CLIENT_ERROR:
          // Client error. Fail!
          DispatchException clientExp = new DispatchException("Write failed on client error");
          asyncWriteRequest.onFailure(clientExp);
          throw clientExp;
        case SERVER_ERROR:
          // Server side error. Retry
          attempt++;
          if (attempt == maxAttempts) {
            DispatchException serverExp = new DispatchException("Write failed after " + maxAttempts + " attempts.");
            asyncWriteRequest.onFailure(serverExp);
            throw serverExp;
          }
      }
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
