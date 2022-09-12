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

import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.UpdateResponse;

import org.apache.gobblin.runtime.api.FlowSpecSearchObject;


public interface FlowConfigsResourceHandler {
  /**
   * Get {@link FlowConfig}
   */
  FlowConfig getFlowConfig(FlowId flowId) throws FlowConfigLoggedException;
  /**
   * Get {@link FlowConfig}
   * @return
   */
  Collection<FlowConfig> getFlowConfig(FlowSpecSearchObject flowSpecSearchObject) throws FlowConfigLoggedException;
  /**
   * Get all {@link FlowConfig}
   */
  Collection<FlowConfig> getAllFlowConfigs();
  /**
   * Get all {@link FlowConfig} with pagination
   */
  Collection<FlowConfig> getAllFlowConfigs(int start, int count);

  /**
   * Add {@link FlowConfig}
   */
  CreateResponse createFlowConfig(FlowConfig flowConfig) throws FlowConfigLoggedException;

  /**
   * Update {@link FlowConfig}
   */
  UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig) throws FlowConfigLoggedException;

  /**
   * Partial update a {@link FlowConfig}
   */
  UpdateResponse partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfig) throws FlowConfigLoggedException;

  /**
   * Delete {@link FlowConfig}
   */
  UpdateResponse deleteFlowConfig(FlowId flowId, Properties header) throws FlowConfigLoggedException;

}
