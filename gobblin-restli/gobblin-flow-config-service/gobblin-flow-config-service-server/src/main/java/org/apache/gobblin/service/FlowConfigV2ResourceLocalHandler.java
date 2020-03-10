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
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.UpdateResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
@Slf4j
public class FlowConfigV2ResourceLocalHandler extends FlowConfigResourceLocalHandler implements FlowConfigsResourceHandler {
  public static final String GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS = "org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler";

  public FlowConfigV2ResourceLocalHandler(FlowCatalog flowCatalog) {
    super(flowCatalog);
  }
  @Override
  /**
   * Add flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  public CreateKVResponse createFlowConfig(FlowConfig flowConfig, boolean triggerListener) throws FlowConfigLoggedException {
    String createLog = "[GAAS-REST] Create called with flowGroup " + flowConfig.getId().getFlowGroup() + " flowName " + flowConfig.getId().getFlowName();
    this.createFlow.mark();

    if (flowConfig.hasExplain()) {
      createLog += " explain " + flowConfig.isExplain();
    }
    log.info(createLog);
    FlowSpec flowSpec = createFlowSpecForConfig(flowConfig);
    FlowStatusId flowStatusId = new FlowStatusId()
        .setFlowName(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_NAME_KEY))
        .setFlowGroup(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_GROUP_KEY));
    if (flowSpec.getConfigAsProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      flowStatusId.setFlowExecutionId(Long.valueOf(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
    } else {
      flowStatusId.setFlowExecutionId(-1L);
    }

    // Return conflict and take no action if flowSpec has already been created
    if (this.flowCatalog.exists(flowSpec.getUri())) {
      log.warn("Flowspec with URI {} already exists, no action will be taken");
      return new CreateKVResponse(new ComplexResourceKey<>(flowConfig.getId(), flowStatusId), flowConfig, HttpStatus.S_409_CONFLICT);
    }

    Map<String, AddSpecResponse> responseMap = this.flowCatalog.put(flowSpec, triggerListener);
    HttpStatus httpStatus = HttpStatus.S_201_CREATED;

    if (flowConfig.hasExplain() && flowConfig.isExplain()) {
      //This is an Explain request. So no resource is actually created.
      //Enrich original FlowConfig entity by adding the compiledFlow to the properties map.
      StringMap props = flowConfig.getProperties();
      AddSpecResponse<String> addSpecResponse = responseMap.getOrDefault(GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS, null);
      props.put("gobblin.flow.compiled",
          addSpecResponse != null && addSpecResponse.getValue() != null ? StringEscapeUtils.escapeJson(addSpecResponse.getValue()) : "");
      flowConfig.setProperties(props);
      //Return response with 200 status code, since no resource is actually created.
      httpStatus = HttpStatus.S_200_OK;
    }
    return new CreateKVResponse(new ComplexResourceKey<>(flowConfig.getId(), flowStatusId), flowConfig, httpStatus);
  }

  @Override
  public UpdateResponse partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException {
    throw new UnsupportedOperationException("Partial update only supported by GobblinServiceFlowConfigResourceHandler");
  }
}
