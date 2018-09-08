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
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
@Slf4j
public class FlowConfigV2ResourceLocalHandler extends FlowConfigResourceLocalHandler implements FlowConfigsResourceHandler {
  public FlowConfigV2ResourceLocalHandler(FlowCatalog flowCatalog) {
    super(flowCatalog);
  }
  @Override
  /**
   * Add flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  public CreateResponse createFlowConfig(FlowConfig flowConfig, boolean triggerListener) throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Create called with flowGroup " + flowConfig.getId().getFlowGroup() + " flowName " + flowConfig.getId().getFlowName());
    FlowSpec flowSpec = createFlowSpecForConfig(flowConfig);
    this.flowCatalog.put(flowSpec, triggerListener);
    FlowStatusId flowStatusId = new FlowStatusId()
        .setFlowName(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_NAME_KEY))
        .setFlowGroup(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_GROUP_KEY));
    if (flowSpec.getConfigAsProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      flowStatusId.setFlowExecutionId(Long.valueOf(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
    } else {
      flowStatusId.setFlowExecutionId(-1L);
    }
    return new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), flowStatusId), HttpStatus.S_201_CREATED);
  }
}
