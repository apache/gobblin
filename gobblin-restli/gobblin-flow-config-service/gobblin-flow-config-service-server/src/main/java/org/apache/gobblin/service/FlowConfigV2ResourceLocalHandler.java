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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.util.PatchApplier;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;


@Slf4j
public class FlowConfigV2ResourceLocalHandler extends FlowConfigResourceLocalHandler implements FlowConfigsV2ResourceHandler {

  @Inject
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
    FlowStatusId flowStatusId =
        new FlowStatusId().setFlowName(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_NAME_KEY))
                          .setFlowGroup(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_GROUP_KEY));
    if (flowSpec.getConfigAsProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      flowStatusId.setFlowExecutionId(Long.valueOf(flowSpec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
    } else {
      flowStatusId.setFlowExecutionId(-1L);
    }

    // Return conflict and take no action if flowSpec has already been created
    if (this.flowCatalog.exists(flowSpec.getUri())) {
      log.warn("FlowSpec with URI {} already exists, no action will be taken", flowSpec.getUri());
      return new CreateKVResponse<>(new RestLiServiceException(HttpStatus.S_409_CONFLICT,
          "FlowSpec with URI " + flowSpec.getUri() + " already exists, no action will be taken"));
    }

    Map<String, AddSpecResponse> responseMap;
    try {
      responseMap = this.flowCatalog.put(flowSpec, triggerListener);
    } catch (QuotaExceededException e) {
        throw new RestLiServiceException(HttpStatus.S_503_SERVICE_UNAVAILABLE, e.getMessage());
    } catch (Throwable e) {
      // TODO: Compilation errors should fall under throwable exceptions as well instead of checking for strings
      log.warn(String.format("Failed to add flow configuration %s.%s to catalog due to", flowConfig.getId().getFlowGroup(), flowConfig.getId().getFlowName()), e);
      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, e.getMessage());
    }
    HttpStatus httpStatus;

    if (flowConfig.hasExplain() && flowConfig.isExplain()) {
      //This is an Explain request. So no resource is actually created.
      //Enrich original FlowConfig entity by adding the compiledFlow to the properties map.
      StringMap props = flowConfig.getProperties();
      AddSpecResponse<String> addSpecResponse = responseMap.getOrDefault(ServiceConfigKeys.COMPILATION_RESPONSE, null);
      props.put("gobblin.flow.compiled",
          addSpecResponse != null && addSpecResponse.getValue() != null ? StringEscapeUtils.escapeJson(addSpecResponse.getValue()) : "");
      flowConfig.setProperties(props);
      httpStatus = HttpStatus.S_200_OK;
    } else if (Boolean.parseBoolean(responseMap.getOrDefault(ServiceConfigKeys.COMPILATION_SUCCESSFUL, new AddSpecResponse<>("false")).getValue().toString())) {
      httpStatus = HttpStatus.S_201_CREATED;
    } else {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, getErrorMessage(flowSpec));
    }

    return new CreateKVResponse<>(new ComplexResourceKey<>(flowConfig.getId(), flowStatusId), flowConfig, httpStatus);
  }

  private String getErrorMessage(FlowSpec flowSpec) {
    StringBuilder message = new StringBuilder("Flow was not compiled successfully.");
    Hashtable<String, ArrayList<String>> allErrors = new Hashtable<>();

    if (!flowSpec.getCompilationErrors().isEmpty()) {
      message.append(" Compilation errors encountered (Sorted by relevance): ");
      FlowSpec.CompilationError[] errors = flowSpec.getCompilationErrors().stream().distinct().toArray(FlowSpec.CompilationError[]::new);
      Arrays.sort(errors, Comparator.comparingInt(c -> ((FlowSpec.CompilationError)c).errorPriority));
      int errorIdSingleHop = 1;
      int errorIdMultiHop = 1;

      ArrayList<String> singleHopErrors = new ArrayList<>();
      ArrayList<String> multiHopErrors = new ArrayList<>();

      for (FlowSpec.CompilationError error: errors) {
        if (error.errorPriority == 0) {
          singleHopErrors.add(String.format("ERROR %s of single-step data movement: ", errorIdSingleHop) + error.errorMessage.replace("\n", " ").replace("\t", ""));
          errorIdSingleHop++;
        } else {
          multiHopErrors.add(String.format("ERROR %s of multi-step data movement: ", errorIdMultiHop) + error.errorMessage.replace("\n", " ").replace("\t", ""));
          errorIdMultiHop++;
        }
      }

      allErrors.put("singleHopErrors", singleHopErrors);
      allErrors.put("multiHopErrors", multiHopErrors);
    }

    allErrors.put("message", new ArrayList<>(Collections.singletonList(message.toString())));
    ObjectMapper mapper = new ObjectMapper();

    try {
      return mapper.writeValueAsString(allErrors);
    } catch (JsonProcessingException e) {
      log.error("Flow Spec {} errored on Json processing", flowSpec.toString(), e);
      e.printStackTrace();
    }
    return "Could not form JSON in FlowConfigV2ResourceLocalHandler";
  }

  /**
   * Update flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  @Override
  public UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig, boolean triggerListener, long modifiedWatermark) {
    log.info("[GAAS-REST] Update called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());

    if (!flowId.getFlowGroup().equals(flowConfig.getId().getFlowGroup()) || !flowId.getFlowName().equals(flowConfig.getId().getFlowName())) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

    FlowConfig originalFlowConfig = getFlowConfig(flowId);

    if (!flowConfig.getProperties().containsKey(RequesterService.REQUESTER_LIST)) {
      // Carry forward the requester list property if it is not being updated since it was added at time of creation
      flowConfig.getProperties().put(RequesterService.REQUESTER_LIST, originalFlowConfig.getProperties().get(RequesterService.REQUESTER_LIST));
    }

    if (isUnscheduleRequest(flowConfig)) {
      // flow config is not changed if it is just a request to un-schedule
      originalFlowConfig.setSchedule(NEVER_RUN_CRON_SCHEDULE);
      flowConfig = originalFlowConfig;
    }

    FlowSpec flowSpec = createFlowSpecForConfig(flowConfig);
    Map<String, AddSpecResponse> responseMap;
    try {
      responseMap = this.flowCatalog.update(flowSpec, triggerListener, modifiedWatermark);
    } catch (QuotaExceededException e) {
      throw new RestLiServiceException(HttpStatus.S_503_SERVICE_UNAVAILABLE, e.getMessage());
    } catch (Throwable e) {
      // TODO: Compilation errors should fall under throwable exceptions as well instead of checking for strings
      log.warn(String.format("Failed to add flow configuration %s.%s to catalog due to", flowId.getFlowGroup(), flowId.getFlowName()), e);
      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, e.getMessage());
    }

    if (Boolean.parseBoolean(responseMap.getOrDefault(ServiceConfigKeys.COMPILATION_SUCCESSFUL, new AddSpecResponse<>("false")).getValue().toString())) {
      return new UpdateResponse(HttpStatus.S_200_OK);
    } else {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, getErrorMessage(flowSpec));
    }
  }



  /**
   * Note: this method is only implemented for testing, normally partial update would be called in
   * GobblinServiceFlowConfigResourceHandler.partialUpdateFlowConfig
   */
  @Override
  public UpdateResponse partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException {
    FlowConfig flowConfig = getFlowConfig(flowId);

    try {
      PatchApplier.applyPatch(flowConfig, flowConfigPatch);
    } catch (DataProcessingException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to apply partial update", e);
    }

    return updateFlowConfig(flowId, flowConfig);
  }
}
