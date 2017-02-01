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
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;


@RestLiCollection(name = "flowconfigs", namespace = "gobblin.service")
/**
 * Rest.li resource for handling flow configuration requests
 */
public class FlowConfigsResource extends ComplexKeyResourceTemplate<FlowConfigId, EmptyRecord, FlowConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigsResource.class);

  @Inject
  private MutableJobCatalog _jobCatalog;

  // For blocking use of this resource until it is ready
  @Inject
  @Named("inUnitTest")
  private boolean inUnitTest;

  FlowConfigsResource() {}

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
   * Retrieve the {@link FlowConfig} with the given key
   * @param key flow config id key containing group name and flow name
   * @return {@link FlowConfig} with flow configuration
   */
  @Override
  public FlowConfig get(ComplexResourceKey<FlowConfigId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();

    LOG.info("Get called with flowGroup " + flowGroup + " flowName " + flowName);

    try {
      JobSpec jobSpec = _jobCatalog.getJobSpec(new URI("/" + flowGroup  + "/" + flowName));
      FlowConfig flowConfig = new FlowConfig();
      Properties flowProps = jobSpec.getConfigAsProperties();

      flowConfig.setSchedule(flowProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
      flowConfig.setTemplateUris(flowProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH));

      if (flowProps.containsKey(ConfigurationKeys.FLOW_RUN_IMMEDIATELY)) {
        flowConfig.setRunImmediately(Boolean.valueOf(flowProps.getProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY)));
      }

      // remove keys that were injected as part of jobSpec creation
      flowProps.remove(ConfigurationKeys.JOB_SCHEDULE_KEY);
      flowProps.remove(ConfigurationKeys.JOB_NAME_KEY);
      flowProps.remove(ConfigurationKeys.JOB_GROUP_KEY);
      flowProps.remove(ConfigurationKeys.JOB_TEMPLATE_PATH);

      StringMap flowPropsAsStringMap = new StringMap();
      flowPropsAsStringMap.putAll(Maps.fromProperties(flowProps));

      return flowConfig.setFlowGroup(flowGroup).setFlowName(flowName).setProperties(flowPropsAsStringMap);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowName, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow requested does not exist: " + flowName, null);
    }

    return null;
  }

  /**
   * Build a {@link JobSpec} from a {@link FlowConfig}
   * @param flowConfig flow configuration
   * @return {@link JobSpec} created with attributes from flowConfig
   */
  private JobSpec createJobSpecForConfig(FlowConfig flowConfig) {
    ConfigBuilder configBuilder = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, flowConfig.getFlowGroup())
        .addPrimitive(ConfigurationKeys.JOB_NAME_KEY, flowConfig.getFlowName())
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, flowConfig.getSchedule());

    if (flowConfig.hasRunImmediately()) {
      configBuilder.addPrimitive(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, flowConfig.isRunImmediately());
    }

    Config config = configBuilder.build();
    Config configWithFallback = config.withFallback(ConfigFactory.parseMap(flowConfig.getProperties()));

    URI templateURI = null;
    try {
      templateURI = new URI(flowConfig.getTemplateUris());
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowConfig.getTemplateUris(), e);
    }

    return JobSpec.builder().withConfig(configWithFallback).withTemplate(templateURI).build();
  }

  /**
   * Put a new {@link JobSpec} based on flowConfig in the {@link MutableJobCatalog}
   * @param flowConfig flow configuration
   * @return {@link CreateResponse}
   */
  @Override
  public CreateResponse create(FlowConfig flowConfig) {
    LOG.info("Create called with flowName " + flowConfig.getFlowName());

    if (!inUnitTest) {
      throw new RuntimeException("Not ready for use.");
    }

    try {
      URI flowUri = new URI("/" + flowConfig.getFlowGroup() + "/" + flowConfig.getFlowName());
      if (_jobCatalog.getJobSpec(flowUri) != null) {
        logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
            "Flow with the same name already exists: " + flowUri, null);
      }
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowConfig.getFlowName(), e);
    } catch (JobSpecNotFoundException e) {
      // okay if flow does not exist
    }

    _jobCatalog.put(createJobSpecForConfig(flowConfig));

    return new CreateResponse(flowConfig.getFlowName(), HttpStatus.S_201_CREATED);
  }

  /**
   * Update the {@link JobSpec} in the {@link MutableJobCatalog} based on the specified {@link FlowConfig}
   * @param key composite key containing group name and flow name that identifies the flow to update
   * @param flowConfig new flow configuration
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<FlowConfigId, EmptyRecord> key, FlowConfig flowConfig) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    String flowUriString = "/" + flowGroup  + "/" + flowName;

    LOG.info("Update called with flowGroup " + flowGroup + " flowName " + flowName);

    if (!flowGroup.equals(flowConfig.getFlowGroup()) || !flowName.equals(flowConfig.getFlowName())) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

    try {
      URI flowUri = new URI(flowUriString);
      JobSpec oldJobSpec = _jobCatalog.getJobSpec(flowUri);
      JobSpec newJobSpec = createJobSpecForConfig(flowConfig);

      _jobCatalog.put(newJobSpec);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUriString, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow does not exist: flowGroup " + flowGroup +
          " flowName " + flowName, null);
    }

    return null;
  }

  /** delete a configured flow
   * @param key composite key containing flow group and flow name that identifies the flow to remove from the
   * {@link MutableJobCatalog}
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowConfigId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    String flowUriString = "/" + flowGroup  + "/" + flowName;

    LOG.info("Delete called with flowGroup " + flowGroup + " flowName " + flowName);

    try {
      URI flowUri = new URI(flowUriString);
      JobSpec jobSpec = _jobCatalog.getJobSpec(flowUri);

      _jobCatalog.remove(flowUri);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + flowUriString, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Flow does not exist: flowGroup " + flowGroup +
          " flowName " + flowName, null);
    }

    return null;
  }
}

