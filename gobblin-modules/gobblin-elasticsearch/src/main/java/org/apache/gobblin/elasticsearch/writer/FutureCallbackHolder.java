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
package org.apache.gobblin.elasticsearch.writer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.util.Pair;
import org.apache.gobblin.writer.GenericWriteResponse;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class to hold Futures and Callbacks to support Async writes
 */
@Slf4j
public class FutureCallbackHolder {

  @Getter
  private final ActionListener<BulkResponse> actionListener;
  private final BlockingQueue<Pair<WriteResponse, Throwable>> writeResponseQueue = new ArrayBlockingQueue<>(1);
  @Getter
  private final Future<WriteResponse> future;
  private final AtomicBoolean done = new AtomicBoolean(false);

  public FutureCallbackHolder(final @Nullable WriteCallback callback,
      ExceptionLogger exceptionLogger,
      final MalformedDocPolicy malformedDocPolicy) {
    this.future = new Future<WriteResponse>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return done.get();
      }

      @Override
      public WriteResponse get()
          throws InterruptedException, ExecutionException {
        Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.take();
        return getWriteResponseorThrow(writeResponseThrowablePair);
      }

      @Override
      public WriteResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.poll(timeout, unit);
        if (writeResponseThrowablePair == null) {
          throw new TimeoutException("Timeout exceeded while waiting for future to be done");
        } else {
          return getWriteResponseorThrow(writeResponseThrowablePair);
        }
      }
    };

    this.actionListener = new ActionListener<BulkResponse>() {
      @Override
      public void onResponse(BulkResponse bulkItemResponses) {
        if (bulkItemResponses.hasFailures()) {
          boolean logicalErrors = false;
          boolean serverErrors = false;
          for (BulkItemResponse bulkItemResponse: bulkItemResponses) {
            if (bulkItemResponse.isFailed()) {
              // check if the failure is permanent (logical) or transient (server)
              if (isLogicalError(bulkItemResponse)) {
                // check error policy
                switch (malformedDocPolicy) {
                  case IGNORE: {
                    log.debug("Document id {} was malformed with error {}",
                        bulkItemResponse.getId(),
                        bulkItemResponse.getFailureMessage());
                    break;
                  }
                  case WARN: {
                    log.warn("Document id {} was malformed with error {}",
                        bulkItemResponse.getId(),
                        bulkItemResponse.getFailureMessage());
                    break;
                  }
                  default: {
                    // Pass through
                  }
                }
                logicalErrors = true;
              } else {
                serverErrors = true;
              }
            }
          }
          if (serverErrors) {
            onFailure(new RuntimeException("Partial failures in the batch: " + bulkItemResponses.buildFailureMessage()));
          } else if (logicalErrors) {
            // all errors found were logical, throw RuntimeException if policy says to Fail
            switch (malformedDocPolicy) {
              case FAIL: {
                onFailure(new RuntimeException("Partial non-recoverable failures in the batch. To ignore these, set "
                    + ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY + " to "
                    + MalformedDocPolicy.IGNORE.name()));
                break;
              }
              default: {
                WriteResponse writeResponse = new GenericWriteResponse<BulkResponse>(bulkItemResponses);
                writeResponseQueue.add(new Pair<WriteResponse, Throwable>(writeResponse, null));
                if (callback != null) {
                  callback.onSuccess(writeResponse);
                }
              }
            }
          }
        } else {
          WriteResponse writeResponse = new GenericWriteResponse<BulkResponse>(bulkItemResponses);
          writeResponseQueue.add(new Pair<WriteResponse, Throwable>(writeResponse, null));
          if (callback != null) {
            callback.onSuccess(writeResponse);
          }
        }
      }

      private boolean isLogicalError(BulkItemResponse bulkItemResponse) {
        String failureMessage = bulkItemResponse.getFailureMessage();
        return failureMessage.contains("IllegalArgumentException")
            || failureMessage.contains("illegal_argument_exception")
            || failureMessage.contains("MapperParsingException")
            || failureMessage.contains("mapper_parsing_exception");
      }

      @Override
      public void onFailure(Exception exception) {
        writeResponseQueue.add(new Pair<WriteResponse, Throwable>(null, exception));
        if (exceptionLogger != null) {
          exceptionLogger.log(exception);
        }
        if (callback != null) {
          callback.onFailure(exception);
        }
      }
    };
  }


  private WriteResponse getWriteResponseorThrow(Pair<WriteResponse, Throwable> writeResponseThrowablePair)
      throws ExecutionException {
    try {
      if (writeResponseThrowablePair.getFirst() != null) {
        return writeResponseThrowablePair.getFirst();
      } else if (writeResponseThrowablePair.getSecond() != null) {
        throw new ExecutionException(writeResponseThrowablePair.getSecond());
      } else {
        throw new ExecutionException(new RuntimeException("Could not find non-null WriteResponse pair"));
      }
    } finally {
      done.set(true);
    }

  }


}
