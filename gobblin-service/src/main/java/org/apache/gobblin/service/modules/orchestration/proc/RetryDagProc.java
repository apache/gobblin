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

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.task.RetryDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation of {@link DagProc} for launching {@link org.apache.gobblin.service.modules.orchestration.task.DagTask}.
 */
@Slf4j
@Alpha
public final class RetryDagProc extends DagProc<Object, Object> {
  private RetryDagTask retryDagTask;
  private Optional<Dag.DagNode<JobExecutionPlan>> dagNodeToRetry;

  public RetryDagProc(RetryDagTask retryDagTask, DagProcessingEngine dagProcFactory) {
    super(dagProcFactory);
    this.retryDagTask = retryDagTask;
  }

  @Override
  protected Object initialize(DagManagementStateStore dagManagementStateStore) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  protected Object act(DagManagementStateStore dagManagementStateStore, Object object) {
    throw new UnsupportedOperationException("Not supported");
  }
}
