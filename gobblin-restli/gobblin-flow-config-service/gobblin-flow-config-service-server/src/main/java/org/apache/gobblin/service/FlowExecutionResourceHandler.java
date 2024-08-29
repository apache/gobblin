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

package org.apache.gobblin.service;

import java.util.List;

import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;


public interface FlowExecutionResourceHandler {
  /**
   * Get {@link FlowExecution}
   */
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key);

  /**
   * Get latest {@link FlowExecution}
   */
  public List<FlowExecution> getLatestFlowExecution(PagingContext context, FlowId flowId, Integer count, String tag,
      String executionStatus, Boolean includeIssues);

  /**
   * Get latest {@link FlowExecution} for every flow in `flowGroup`
   *
   * NOTE: `executionStatus` param not provided yet, without justifying use case, due to complexity of interaction with `countPerFlow`
   * and resulting efficiency concern of performing across many flows sharing the single named group.
   */
  public List<FlowExecution> getLatestFlowGroupExecutions(PagingContext context, String flowGroup, Integer countPerFLow,
      String tag, Boolean includeIssues);

  /**
   * Resume a failed {@link FlowExecution} from the point before failure
   */
  public void resume(ComplexResourceKey<FlowStatusId, EmptyRecord> key);

  /**
   * Kill a running {@link FlowExecution}
   */
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key);
}
