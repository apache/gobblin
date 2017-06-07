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

package gobblin.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.spec_catalog.FlowCatalog;

/**
 * Resource for handling flow configuration requests
 */
@RestLiCollection(name = "flowconfigs", namespace = "gobblin.service", keyName = "id")
public class FlowConfigsResource extends ComplexKeyResourceTemplate<FlowId, EmptyRecord, FlowConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigsResource.class);

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("MS_SHOULD_BE_FINAL")
  public static FlowCatalog _globalFlowCatalog;

  @Inject
  @Named("flowCatalog")
  private FlowCatalog _flowCatalog;

  // For blocking use of this resource until it is ready
  @Inject
  @Named("readyToUse")
  private Boolean readyToUse = Boolean.FALSE;

  public FlowConfigsResource() {}

  /**
   * Logs message and throws Rest.li exception
   * @param status HTTP status code
   * @param msg error message
   * @param e exception
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg, Exception e) {
    if (e != null) {
      LOG.error(msg, e);
      throw new RestLiServiceException(status, msg + " cause = " + e.getMessage());
    } else {
      LOG.error(msg);
      throw new RestLiServiceException(status, msg);
    }
  }

  /**
   * Retrieve the flow configuration with the given key
   * @param key flow config id key containing group name and flow name
   * @return {@link FlowConfig} with flow configuration
   */
  @Override
  public FlowConfig get(ComplexResourceKey<FlowId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();

    LOG.info("Get called with flowGroup " + flowGroup + " flowName " + flowName);

    try {
      URI flowCatalogURI = new URI("gobblin-flow", null, "/", null, null);
      URI flowUri = new URI(flowCatalogURI.getScheme(), flowCatalogURI.getAuthority(),
          "/" + flowGroup + "/" + flowName, null, null);
      FlowSpec spec = (FlowSpec) getFlowCatalog().getSpec(flowUri);
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

      return flowConfig.setId(new FlowId().setFlowGroup(flowGroup).setFlowName(flowName))
          .setProperties(flowPropsAsStringMap);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowName, e);
    } catch (SpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow requested does not exist: " + flowName, null);
    }

    return null;
  }

  /**
   * Build a {@link FlowSpec} from a {@link FlowConfig}
   * @param flowConfig flow configuration
   * @return {@link FlowSpec} created with attributes from flowConfig
   */
  private FlowSpec createFlowSpecForConfig(FlowConfig flowConfig) {
    ConfigBuilder configBuilder = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, flowConfig.getId().getFlowGroup())
        .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, flowConfig.getId().getFlowName());

    if (flowConfig.hasSchedule()) {
      Schedule schedule = flowConfig.getSchedule();
      configBuilder.addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, schedule.getCronSchedule());
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, schedule.isRunImmediately());
    }

    Config config = configBuilder.build();
    Config configWithFallback = config.withFallback(ConfigFactory.parseMap(flowConfig.getProperties()));

    URI templateURI = null;
    try {
      templateURI = new URI(flowConfig.getTemplateUris());
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowConfig.getTemplateUris(), e);
    }

    return FlowSpec.builder().withConfig(configWithFallback).withTemplate(templateURI).build();
  }

  /**
   * Create a flow configuration that the service will forward to execution instances for execution
   * @param flowConfig flow configuration
   * @return {@link CreateResponse}
   */
  @Override
  public CreateResponse create(FlowConfig flowConfig) {
    LOG.info("Create called with flowName " + flowConfig.getId().getFlowName());

    LOG.info("ReadyToUse is: " + readyToUse);
    LOG.info("FlowCatalog is: " + getFlowCatalog());

    if (!readyToUse && getFlowCatalog() == null) {
      throw new RuntimeException("Not ready for use.");
    }

    try {
      URI flowCatalogURI = new URI("gobblin-flow", null, "/", null, null);
      URI flowUri = new URI(flowCatalogURI.getScheme(), flowCatalogURI.getAuthority(),
          "/" + flowConfig.getId().getFlowGroup() + "/" + flowConfig.getId().getFlowName(), null, null);

      if (getFlowCatalog().getSpec(flowUri) != null) {
        logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
            "Flow with the same name already exists: " + flowUri, null);
      }
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowConfig.getId().getFlowName(), e);
    } catch (SpecNotFoundException e) {
      // okay if flow does not exist
    }

    getFlowCatalog().put(createFlowSpecForConfig(flowConfig));

    return new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()), HttpStatus.S_201_CREATED);
  }

  /**
   * Update the flow configuration with the specified key. Running flows are not affected.
   * An error is raised if the flow configuration does not exist.
   * @param key composite key containing group name and flow name that identifies the flow to update
   * @param flowConfig new flow configuration
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<FlowId, EmptyRecord> key, FlowConfig flowConfig) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    URI flowUri = null;

    LOG.info("Update called with flowGroup " + flowGroup + " flowName " + flowName);

    if (!flowGroup.equals(flowConfig.getId().getFlowGroup()) || !flowName.equals(flowConfig.getId().getFlowName())) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

    try {
      URI flowCatalogURI = new URI("gobblin-flow", null, "/", null, null);
      flowUri = new URI(flowCatalogURI.getScheme(), flowCatalogURI.getAuthority(),
          "/" + flowGroup + "/" + flowName, null, null);
      FlowSpec oldFlowSpec = (FlowSpec) getFlowCatalog().getSpec(flowUri);
      FlowSpec newFlowSpec = createFlowSpecForConfig(flowConfig);

      getFlowCatalog().put(newFlowSpec);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUri, e);
    } catch (SpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow does not exist: flowGroup " + flowGroup +
          " flowName " + flowName, null);
    }

    return null;
  }

  /**
   * Delete a configured flow. Running flows are not affected. The schedule will be removed for scheduled flows.
   * @param key composite key containing flow group and flow name that identifies the flow to remove from the
   * {@link FlowCatalog}
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    URI flowUri = null;

    LOG.info("Delete called with flowGroup " + flowGroup + " flowName " + flowName);

    try {
      URI flowCatalogURI = new URI("gobblin-flow", null, "/", null, null);
      flowUri = new URI(flowCatalogURI.getScheme(), flowCatalogURI.getAuthority(),
          "/" + flowGroup + "/" + flowName, null, null);
      FlowSpec flowSpec = (FlowSpec) getFlowCatalog().getSpec(flowUri);

      getFlowCatalog().remove(flowUri);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUri, e);
    } catch (SpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow does not exist: flowGroup " + flowGroup +
          " flowName " + flowName, null);
    }

    return null;
  }

  /***
   * This method is to workaround injection issues where Service has only one active global FlowCatalog
   * .. and is not able to inject it via RestLI bootstrap. We should remove this and make injected
   * .. FlowCatalog standard after injection works and recipe is documented here.
   * @return FlowCatalog in use.
   */
  private FlowCatalog getFlowCatalog() {
    if (null != _globalFlowCatalog) {
      return _globalFlowCatalog;
    }
    return this._flowCatalog;
  }
}

