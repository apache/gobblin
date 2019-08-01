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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.util.PatchApplier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.naming.OperationNotSupportedException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A {@link FlowConfigsResourceHandler} that handles Restli locally.
 */
@Slf4j
public class FlowConfigResourceLocalHandler implements FlowConfigsResourceHandler {
  public static final Schedule NEVER_RUN_CRON_SCHEDULE = new Schedule().setCronSchedule("0 0 0 ? 1 1 2050");
  @Getter
  protected FlowCatalog flowCatalog;
  protected final ContextAwareMeter createFlow;
  protected final ContextAwareMeter deleteFlow;
  protected final ContextAwareMeter runImmediatelyFlow;

  public FlowConfigResourceLocalHandler(FlowCatalog flowCatalog) {
    this.flowCatalog = flowCatalog;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.createFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(MetricReportUtils.GOBBLIN_SERVICE_METRICS_PREFIX, ServiceMetricNames.CREATE_FLOW_METER));
    this.deleteFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(MetricReportUtils.GOBBLIN_SERVICE_METRICS_PREFIX, ServiceMetricNames.DELETE_FLOW_METER));
    this.runImmediatelyFlow = metricContext.contextAwareMeter(
        MetricRegistry.name(MetricReportUtils.GOBBLIN_SERVICE_METRICS_PREFIX, ServiceMetricNames.RUN_IMMEDIATELY_FLOW_METER));
  }

  /**
   * Get flow config
   */
  public FlowConfig getFlowConfig(FlowId flowId) throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Get called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());

    try {
      URI flowUri = FlowUriUtils.createFlowSpecUri(flowId);
      FlowSpec spec = (FlowSpec) flowCatalog.getSpec(flowUri);
      FlowConfig flowConfig = new FlowConfig();
      Properties flowProps = spec.getConfigAsProperties();
      Schedule schedule = null;

      if (flowProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        schedule = new Schedule();
        schedule.setCronSchedule(flowProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
      }
      if (flowProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
        flowConfig.setTemplateUris(flowProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH));
      } else if (spec.getTemplateURIs().isPresent()) {
        flowConfig.setTemplateUris(StringUtils.join(spec.getTemplateURIs().get(), ","));
      } else {
        flowConfig.setTemplateUris("NA");
      }
      if (schedule != null) {
        if (flowProps.containsKey(ConfigurationKeys.FLOW_RUN_IMMEDIATELY)) {
          schedule.setRunImmediately(Boolean.valueOf(flowProps.getProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY)));
        }

        flowConfig.setSchedule(schedule);
      }

      // remove keys that were injected as part of flowSpec creation
      flowProps.remove(ConfigurationKeys.JOB_SCHEDULE_KEY);
      flowProps.remove(ConfigurationKeys.JOB_TEMPLATE_PATH);

      StringMap flowPropsAsStringMap = new StringMap();
      flowPropsAsStringMap.putAll(Maps.fromProperties(flowProps));

      return flowConfig.setId(new FlowId().setFlowGroup(flowId.getFlowGroup()).setFlowName(flowId.getFlowName()))
          .setProperties(flowPropsAsStringMap);
    } catch (URISyntaxException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowId.getFlowName(), e);
    } catch (SpecNotFoundException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_404_NOT_FOUND, "Flow requested does not exist: " + flowId.getFlowName(), null);
    }
  }

  /**
   * Add flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  public CreateResponse createFlowConfig(FlowConfig flowConfig, boolean triggerListener) throws FlowConfigLoggedException {
    log.info("[GAAS-REST] Create called with flowGroup " + flowConfig.getId().getFlowGroup() + " flowName " + flowConfig.getId().getFlowName());
    this.createFlow.mark();
    if (!flowConfig.hasSchedule() || StringUtils.isEmpty(flowConfig.getSchedule().getCronSchedule())) {
      this.runImmediatelyFlow.mark();
    }

    if (flowConfig.hasExplain()) {
      //Return Error if FlowConfig has explain set. Explain request is only valid for v2 FlowConfig.
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "FlowConfig with explain not supported."));
    }

    FlowSpec flowSpec = createFlowSpecForConfig(flowConfig);
    // Existence of a flow spec in the flow catalog implies that the flow is currently running.
    // If the new flow spec has a schedule we should allow submission of the new flow to accept the new schedule.
    // However, if the new flow spec does not have a schedule, we should allow submission only if it is not running.
    if (!flowConfig.hasSchedule() && this.flowCatalog.exists(flowSpec.getUri())) {
      return new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()), HttpStatus.S_409_CONFLICT);
    } else {
      this.flowCatalog.put(flowSpec, triggerListener);
      return new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()), HttpStatus.S_201_CREATED);
    }
  }

  /**
   * Add flowConfig locally and trigger all listeners
   */
  public CreateResponse createFlowConfig(FlowConfig flowConfig) throws FlowConfigLoggedException {
    return this.createFlowConfig(flowConfig, true);
  }

  /**
   * Update flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  public UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig, boolean triggerListener) {
    log.info("[GAAS-REST] Update called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());

    if (!flowId.getFlowGroup().equals(flowConfig.getId().getFlowGroup()) || !flowId.getFlowName().equals(flowConfig.getId().getFlowName())) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }
    if (isUnscheduleRequest(flowConfig)) {
      // flow config is not changed if it is just a request to un-schedule
      FlowConfig originalFlowConfig = getFlowConfig(flowId);
      originalFlowConfig.setSchedule(NEVER_RUN_CRON_SCHEDULE);
      flowConfig = originalFlowConfig;
    }

    this.flowCatalog.put(createFlowSpecForConfig(flowConfig), triggerListener);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  private boolean isUnscheduleRequest(FlowConfig flowConfig) {
    return Boolean.parseBoolean(flowConfig.getProperties().getOrDefault(ConfigurationKeys.FLOW_UNSCHEDULE_KEY, "false"));
  }

  /**
   * Update flowConfig locally and trigger all listeners
   */
  public UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig) throws FlowConfigLoggedException {
    return updateFlowConfig(flowId, flowConfig, true);
  }

  @Override
  public UpdateResponse partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException {
    throw new UnsupportedOperationException("Partial update only supported by GobblinServiceFlowConfigResourceHandler");
  }

  /**
   * Delete flowConfig locally and trigger all listeners iff @param triggerListener is set to true
   */
  public UpdateResponse deleteFlowConfig(FlowId flowId, Properties header, boolean triggerListener) throws FlowConfigLoggedException {

    log.info("[GAAS-REST] Delete called with flowGroup {} flowName {}", flowId.getFlowGroup(), flowId.getFlowName());
    this.deleteFlow.mark();
    URI flowUri = null;

    try {
      flowUri = FlowUriUtils.createFlowSpecUri(flowId);
      this.flowCatalog.remove(flowUri, header, triggerListener);
      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUri, e);
    }
  }

  /**
   * Delete flowConfig locally and trigger all listeners
   */
  public UpdateResponse deleteFlowConfig(FlowId flowId, Properties header)  throws FlowConfigLoggedException {
    return deleteFlowConfig(flowId, header, true);
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

    Config config = configBuilder.build();

    Config configWithFallback;
    //We first attempt to process the REST.li request as a HOCON string. If the request is not a valid HOCON string
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

  public static class FlowUriUtils {
    private final static String URI_SCHEME = "gobblin-flow";
    private final static String URI_AUTHORITY = null;
    private final static String URI_PATH_SEPARATOR = "/";
    private final static String URI_QUERY = null;
    private final static String URI_FRAGMENT = null;
    private final static int EXPECTED_NUM_URI_PATH_TOKENS = 3;

    public static URI createFlowSpecUri(FlowId flowId) throws URISyntaxException {
      return new URI(URI_SCHEME, URI_AUTHORITY, createUriPath(flowId), URI_QUERY, URI_FRAGMENT);
    }

    private static String createUriPath(FlowId flowId) {
      return URI_PATH_SEPARATOR + flowId.getFlowGroup() + URI_PATH_SEPARATOR + flowId.getFlowName();
    }

    /**
     * returns the flow name from the flowUri
     * @param flowUri FlowUri
     * @return null if the provided flowUri is not valid
     */
    public static String getFlowName(URI flowUri) {
      String[] uriTokens = flowUri.getPath().split("/");
      if (uriTokens.length != EXPECTED_NUM_URI_PATH_TOKENS) {
        log.error("Invalid URI {}.", flowUri);
        return null;
      }
      return uriTokens[EXPECTED_NUM_URI_PATH_TOKENS - 1];
    }

    /**
     * returns the flow group from the flowUri
     * @param flowUri FlowUri
     * @return null if the provided flowUri is not valid
     */
    public static String getFlowGroup(URI flowUri) {
      String[] uriTokens = flowUri.getPath().split("/");
      if (uriTokens.length != EXPECTED_NUM_URI_PATH_TOKENS) {
        log.error("Invalid URI {}.", flowUri);
        return null;
      }
      return uriTokens[EXPECTED_NUM_URI_PATH_TOKENS - 2];
    }
  }
}
