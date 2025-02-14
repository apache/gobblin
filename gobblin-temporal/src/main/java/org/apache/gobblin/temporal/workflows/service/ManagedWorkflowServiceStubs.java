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

package org.apache.gobblin.temporal.workflows.service;

import java.io.Closeable;

import io.temporal.serviceclient.WorkflowServiceStubs;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A wrapper class of {@link WorkflowServiceStubs} that implements the Closeable interface.
 * It manages the lifecycle of {@link WorkflowServiceStubs}, ensuring proper shutdown and resource cleanup.
 */
@Getter
@Slf4j
public class ManagedWorkflowServiceStubs implements Closeable {
  private final WorkflowServiceStubs workflowServiceStubs;

  public ManagedWorkflowServiceStubs(WorkflowServiceStubs serviceStubs) {
    this.workflowServiceStubs = serviceStubs;
  }

  @Override
  public void close() {
    try {
      try {
        workflowServiceStubs.getOptions().getMetricsScope().close();
      }
      finally {
        workflowServiceStubs.shutdown();
      }
    }
    catch (Exception e) {
      log.error("Exception occurred while closing ManagedWorkflowServiceStubs", e);
    }
  }
}
