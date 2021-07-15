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

package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.util.CompletedFuture;


/**
 * Defines a SpecProducer to produce jobs to {@link SpecExecutor}
 * that can execute a {@link Spec}.
 *
 * A handle on the Orchestrator side to send {@link Spec}s.
 */
@Alpha
public interface SpecProducer<V> {
  /** Add a {@link Spec} for execution on {@link SpecExecutor}. */
  Future<?> addSpec(V addedSpec);

  /** Update a {@link Spec} being executed on {@link SpecExecutor}. */
  Future<?> updateSpec(V updatedSpec);

  default Future<?> deleteSpec(URI deletedSpecURI) {
    return deleteSpec(deletedSpecURI, new Properties());
  }

  /** Delete a {@link Spec} being executed on {@link SpecExecutor}. */
  Future<?> deleteSpec(URI deletedSpecURI, Properties headers);

  /** List all {@link Spec} being executed on {@link SpecExecutor}. */
  Future<? extends List<V>> listSpecs();

  default String getExecutionLink(Future<?> future, String specExecutorUri) {
    return "";
  }

  default String serializeAddSpecResponse(Future<?> response) {
    return "";
  }

  default Future<?> deserializeAddSpecResponse(String serializedResponse) {
    return new CompletedFuture(serializedResponse, null);
  }

  /** Cancel the job execution identified by jobURI */
  default Future<?> cancelJob(URI jobURI, Properties properties) {
      return new CompletedFuture<>(jobURI, null);
  }
}