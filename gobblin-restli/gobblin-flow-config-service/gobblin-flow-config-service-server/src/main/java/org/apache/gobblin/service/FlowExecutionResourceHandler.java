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

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;

import org.apache.gobblin.runtime.api.FlowSpecSearchObject;


public interface FlowExecutionResourceHandler {
  /**
   * Get {@link FlowExecution}
   */
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key);

  /**
   * Get latest {@link FlowExecution}
   */
  public List<FlowExecution> getLatestFlowExecution(PagingContext context, FlowId flowId, Integer count, String tag, String executionStatus);

  /**
   * Kill a running {@link FlowExecution}
   */
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key);
}
