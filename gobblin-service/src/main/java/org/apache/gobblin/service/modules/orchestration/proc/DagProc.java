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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;


/**
 * Responsible to performing the actual work for a given {@link DagTask}.
 * It processes the {@link DagTask} by first initializing its state, performing actions
 * based on the type of {@link DagTask} and finally submitting an event to the executor.
 * @param <S> current state of the dag node
 * @param <R> result after processing the dag node
 */
@Alpha
@Slf4j
public abstract class DagProc<S, R> {

  abstract protected S initialize(DagManagementStateStore dagManagementStateStore) throws MaybeRetryableException, IOException;
  abstract protected R act(S state, DagManagementStateStore dagManagementStateStore) throws MaybeRetryableException, Exception;
  abstract protected void sendNotification(R result, EventSubmitter eventSubmitter) throws MaybeRetryableException, IOException;

  public final void process(DagManagementStateStore dagManagementStateStore, EventSubmitter eventSubmitter, int maxRetryCount, long delayRetryMillis) {
    try {
      S state = this.initializeWithRetries(dagManagementStateStore, maxRetryCount, delayRetryMillis);
      R result = this.actWithRetries(state, dagManagementStateStore, maxRetryCount, delayRetryMillis); // may be pass state store too here
      this.sendNotificationWithRetries(result, eventSubmitter, maxRetryCount, delayRetryMillis);
      log.info("Successfully processed Dag Request");
    } catch (Exception ex) {
      throw new RuntimeException("Cannot process Dag Request: ", ex);
    }
  }

  protected final S initializeWithRetries(DagManagementStateStore dagManagementStateStore, int maxRetryCount, long delayRetryMillis) throws IOException {
    for (int retryCount = 0; retryCount < maxRetryCount; retryCount++) {
      try {
        return this.initialize(dagManagementStateStore);
      } catch (MaybeRetryableException e) {
        if (retryCount < maxRetryCount - 1) { // Don't wait before the last retry
          waitBeforeRetry(delayRetryMillis);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    throw new RuntimeException("Max retry attempts reached. Cannot initialize Dag");
  }

  protected final R actWithRetries(S state, DagManagementStateStore dagManagementStateStore, int maxRetryCount, long delayRetryMillis) {
    for (int retryCount = 0; retryCount < maxRetryCount; retryCount++) {
      try {
        return this.act(state, dagManagementStateStore);
      } catch (MaybeRetryableException e) {
        if (retryCount < maxRetryCount - 1) { // Don't wait before the last retry
          waitBeforeRetry(delayRetryMillis);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    throw new RuntimeException("Max retry attempts reached. Cannot act on the Dag");
  }

  protected final void sendNotificationWithRetries(R result, EventSubmitter eventSubmitter, int maxRetryCount, long delayRetryMillis) {
    for (int retryCount = 0; retryCount < maxRetryCount; retryCount++) {
      try {
        this.sendNotification(result, eventSubmitter);
        return;
      } catch (MaybeRetryableException e) {
        if (retryCount < maxRetryCount - 1) { // Don't wait before the last retry
          waitBeforeRetry(delayRetryMillis);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    throw new RuntimeException("Max retry attempts reached. Cannot send notification for the Dag");
  }

  private void waitBeforeRetry(long delayRetryMillis) {
    try {
      TimeUnit.MILLISECONDS.sleep(delayRetryMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
