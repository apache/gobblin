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

package org.apache.gobblin.service.modules.restli;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringEscapeUtils;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.util.PatchApplier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Inject;
import javax.inject.Named;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.FlowSpecSearchObject;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigLoggedException;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.FlowStatusId;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class FlowConfigsV2ResourceHandler {

  @Getter
  private String serviceName;
  public static final Schedule NEVER_RUN_CRON_SCHEDULE = new Schedule().setCronSchedule("0 0 0 ? 1 1 2050");
  @Getter
  protected FlowCatalog flowCatalog;
  protected final ContextAwareMeter createFlow;
  protected final ContextAwareMeter deleteFlow;
  protected final ContextAwareMeter runImmediatelyFlow;

  @Inject
  public FlowConfigsV2ResourceHandler(@Named(InjectionNames.SERVICE_NAME) String serviceName, FlowCatalog flowCatalog) {
    this.serviceName = serviceName;
    this.flowCatalog = flowCatalog;
    MetricContext
        metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.createFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.CREATE_FLOW_METER));
    this.deleteFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.DELETE_FLOW_METER));
    this.runImmediatelyFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.RUN_IMMEDIATELY_FLOW_METER));
  }

  public FlowConfig getFlowConfig(FlowId flowId)
      throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Get called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());

    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      FlowSpec spec = flowCatalog.getSpecs(flowUri);
      return FlowSpec.Utils.toFlowConfig(spec);
    } catch (URISyntaxException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowId.getFlowName(), e);
    } catch (SpecNotFoundException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_404_NOT_FOUND, "Flow requested does not exist: " + flowId.getFlowName(), null);
    }
  }

  public Collection<FlowConfig> getFlowConfig(FlowSpecSearchObject flowSpecSearchObject) throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Get called with flowSpecSearchObject {}", flowSpecSearchObject);
    return flowCatalog.getSpecs(flowSpecSearchObject).stream().map(FlowSpec.Utils::toFlowConfig).collect(Collectors.toList());

  }

  public Collection<FlowConfig> getAllFlowConfigs() {
    log.info("[GAAS-REST] GetAll called");
    return flowCatalog.getAllSpecs().stream().map(FlowSpec.Utils::toFlowConfig).collect(Collectors.toList());

  }

  public Collection<FlowConfig> getAllFlowConfigs(int start, int count) {
    return flowCatalog.getSpecsPaginated(start, count).stream().map(FlowSpec.Utils::toFlowConfig).collect(Collectors.toList());
  }



  public UpdateResponse deleteFlowConfig(FlowId flowId, Properties header)
      throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Delete called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());
    this.deleteFlow.mark();
    URI flowUri = null;

    try {
      flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      this.flowCatalog.remove(flowUri, header, true);
      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUri, e);
    }  }

  public UpdateResponse  partialUpdateFlowConfig(FlowId flowId,
      PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException {
    long modifiedWatermark = System.currentTimeMillis() / 1000;
    FlowConfig flowConfig = getFlowConfig(flowId);

    try {
      PatchApplier.applyPatch(flowConfig, flowConfigPatch);
    } catch (DataProcessingException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to apply partial update", e);
    }

    return updateFlowConfig(flowId, flowConfig, modifiedWatermark);
  }

  public UpdateResponse updateFlowConfig(FlowId flowId,
      FlowConfig flowConfig) throws FlowConfigLoggedException {
    // We have modifiedWatermark here to avoid update config happens at the same time on different hosts overwrite each other
    // timestamp here will be treated as largest modifiedWatermark that we can update
    long version = System.currentTimeMillis() / 1000;
    return updateFlowConfig(flowId, flowConfig, version);
  }

  public UpdateResponse updateFlowConfig(FlowId flowId,
      FlowConfig flowConfig, long modifiedWatermark) throws FlowConfigLoggedException {
    String flowName = flowId.getFlowName();
    String flowGroup = flowId.getFlowGroup();

    if (!flowGroup.equals(flowConfig.getId().getFlowGroup()) || !flowName.equals(flowConfig.getId().getFlowName())) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

    // We directly call localHandler to create flow config and put it in spec store

    //Instead of helix message, forwarding message is done by change stream of spec store

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
      responseMap = this.flowCatalog.update(flowSpec, true, modifiedWatermark);
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

  public CreateKVResponse<ComplexResourceKey<FlowId, FlowStatusId>, FlowConfig> createFlowConfig(FlowConfig flowConfig) throws FlowConfigLoggedException {
    if (flowConfig.getProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          String.format("%s cannot be set by the user", ConfigurationKeys.FLOW_EXECUTION_ID_KEY), null);
    }

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
      responseMap = this.flowCatalog.put(flowSpec, true);
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

  private boolean isUnscheduleRequest(FlowConfig flowConfig) {
    return Boolean.parseBoolean(flowConfig.getProperties().getOrDefault(ConfigurationKeys.FLOW_UNSCHEDULE_KEY, "false"));
  }

  /**
   * Build a {@link FlowSpec} from a {@link FlowConfig}
   * @param flowConfig flow configuration
   * @return {@link FlowSpec} created with attributes from flowConfig
   */
  public static FlowSpec createFlowSpecForConfig(FlowConfig flowConfig) {
    ConfigBuilder configBuilder = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, flowConfig.getId().getFlowGroup())
        .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, flowConfig.getId().getFlowName());

    if (flowConfig.hasSchedule()) {
      Schedule schedule = flowConfig.getSchedule();
      configBuilder.addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, schedule.getCronSchedule());
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, schedule.isRunImmediately());
    } else {
      // If the job does not have schedule, it is a run-once job.
      // In this case, we add flow execution id to the flow spec now to be able to send this id back to the user for
      // flow status tracking purpose.
      // If it is not a run-once job, we should not add flow execution id here,
      // because execution id is generated for every scheduled execution of the flow and cannot be materialized to
      // the flow catalog. In this case, this id is added during flow compilation.
      String flowExecutionId;
      if (flowConfig.getProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
        flowExecutionId = flowConfig.getProperties().get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
        // FLOW_EXECUTION_ID may already be present in FlowSpec in cases
        // where the FlowSpec is forwarded by a slave to the master.
        log.info("Using the existing flowExecutionId {} for {},{}", flowExecutionId, flowConfig.getId().getFlowGroup(), flowConfig.getId().getFlowName());
      } else {
        flowExecutionId = String.valueOf(System.currentTimeMillis());
        log.info("Created a flowExecutionId {} for {},{}", flowExecutionId, flowConfig.getId().getFlowGroup(), flowConfig.getId().getFlowName());
      }
      flowConfig.getProperties().put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
    }

    if (flowConfig.hasExplain()) {
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_EXPLAIN_KEY, flowConfig.isExplain());
    }

    if (flowConfig.hasOwningGroup()) {
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_OWNING_GROUP_KEY, flowConfig.getOwningGroup());
    }

    Config config = configBuilder.build();

    Config configWithFallback;
    // We first attempt to process the REST.li request as a HOCON string. If the request is not a valid HOCON string
    // (e.g. when certain special characters such as ":" or "*" are not properly escaped), we catch the Typesafe ConfigException and
    // fallback to assuming that values are literal strings.
    try {
      // We first convert the StringMap object to a String object and then use ConfigFactory#parseString() to parse the
      // HOCON string.
      configWithFallback = config.withFallback(ConfigFactory.parseString(flowConfig.getProperties().toString()).resolve());
    } catch (Exception e) {
      configWithFallback = config.withFallback(ConfigFactory.parseMap(flowConfig.getProperties()));
    }

    try {
      URI templateURI = new URI(flowConfig.getTemplateUris());
      return FlowSpec.builder().withConfig(configWithFallback).withTemplate(templateURI).build();
    } catch (URISyntaxException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowConfig.getTemplateUris(), e);
    }
  }

  protected String getErrorMessage(FlowSpec flowSpec) {
    StringBuilder message = new StringBuilder("Flow was not compiled successfully.");
    Map<String, ArrayList<String>> allErrors = new HashMap<>();

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
      log.error(String.format("FlowSpec %s errored on Json processing", flowSpec.toString()), e);
    }
    return "Could not form JSON in " + getClass().getSimpleName();
  }
}
