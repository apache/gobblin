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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;


/**
 * Responsible to performing the actual work for a given {@link DagTask}.
 * It processes the {@link DagTask} by first initializing its state, performing actions
 * based on the type of {@link DagTask} and finally submitting an event to the executor.
 * @param <S> current state of the dag node
 * @param <R> result after processing the dag node
 */
@WorkInProgress
public abstract class DagProc<S, R> {
  abstract protected S initialize() throws MaybeRetryableException;
  abstract protected R act(S state) throws ExecutionException, InterruptedException, IOException;
  abstract protected void sendNotification(R result) throws MaybeRetryableException;

  void process(MultiActiveLeaseArbiter.LeaseAttemptStatus leaseStatus) {
  throw new UnsupportedOperationException(" Process unsupported");
  }
}
