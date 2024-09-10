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
import java.util.Properties;

import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.UpdateResponse;

import org.apache.gobblin.runtime.api.FlowSpecSearchObject;


// This is an interface rather than a class because implementation may need resources from the packages it cannot have
// direct dependency on
/**
 * It is closely coupled with {@link FlowConfigsV2Resource} and handle all the requests it get.
 */
public interface FlowConfigsResourceHandlerInterface {

  FlowConfig getFlowConfig(FlowId flowId) throws FlowConfigLoggedException;

  Collection<FlowConfig> getFlowConfig(FlowSpecSearchObject flowSpecSearchObject) throws FlowConfigLoggedException;

  /**
   * Get all flow configs
   */
  Collection<FlowConfig> getAllFlowConfigs();

  /**
   * Get all flow configs in between start and start + count - 1
   */
  Collection<FlowConfig> getAllFlowConfigs(int start, int count);

  UpdateResponse deleteFlowConfig(FlowId flowId, Properties header) throws FlowConfigLoggedException;

  UpdateResponse  partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException;

  UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig) throws FlowConfigLoggedException;

  CreateKVResponse<ComplexResourceKey<FlowId, FlowStatusId>, FlowConfig> createFlowConfig(FlowConfig flowConfig) throws FlowConfigLoggedException;
}
