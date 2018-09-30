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

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.service.monitoring.FlowStatusGenerator;

import static org.apache.gobblin.service.FlowStatusResource.FLOW_STATUS_GENERATOR_INJECT_NAME;


/**
 * Resource for handling flow configuration requests
 */
@RestLiCollection(name = "flowconfigs", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowConfigsResource extends ComplexKeyResourceTemplate<FlowId, EmptyRecord, FlowConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigsResource.class);
  public static final String FLOW_CONFIG_GENERATOR_INJECT_NAME = "flowConfigsResourceHandler";
  private static final Set<String> ALLOWED_METADATA = ImmutableSet.of("delete.state.store");

  @Inject @javax.inject.Inject @javax.inject.Named(FLOW_STATUS_GENERATOR_INJECT_NAME)
  FlowStatusGenerator _flowStatusGenerator;


  @edu.umd.cs.findbugs.annotations.SuppressWarnings("MS_SHOULD_BE_FINAL")
  public static FlowConfigsResourceHandler global_flowConfigsResourceHandler = null;

  @Inject
  @Named(FLOW_CONFIG_GENERATOR_INJECT_NAME)
  private FlowConfigsResourceHandler flowConfigsResourceHandler;

  // For blocking use of this resource until it is ready
  @Inject
  @Named("readyToUse")
  private Boolean readyToUse = Boolean.FALSE;

  public FlowConfigsResource() {
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
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().getFlowConfig(flowId);
  }

  /**
   * Create a flow configuration that the service will forward to execution instances for execution
   * @param flowConfig flow configuration
   * @return {@link CreateResponse}
   */
  @Override
  public CreateResponse create(FlowConfig flowConfig) {
    ExecutionStatus latestFlowExecutionStatus = getLatestExecutionStatus(flowConfig);
    if (latestFlowExecutionStatus == ExecutionStatus.RUNNING) {
      LOG.warn("Last execution of this flow is still running, not submitting this flow config.");
      return new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()), HttpStatus.S_409_CONFLICT);
    } else {
      return this.getFlowConfigResourceHandler().createFlowConfig(flowConfig);
    }
  }

  private ExecutionStatus getLatestExecutionStatus(FlowConfig flowConfig) {
    org.apache.gobblin.service.monitoring.FlowStatus latestFlowStatus =
        _flowStatusGenerator.getLatestFlowStatus(flowConfig.getId().getFlowName(), flowConfig.getId().getFlowGroup());

    if (latestFlowStatus == null) {
      return ExecutionStatus.$UNKNOWN;
    }

    Iterator<org.apache.gobblin.service.monitoring.JobStatus> jobStatusIterator = latestFlowStatus.getJobStatusIterator();

    ExecutionStatus latestFlowExecutionStatus = ExecutionStatus.COMPLETE;

    while(jobStatusIterator.hasNext()) {
      latestFlowExecutionStatus = FlowStatusResource.updatedFlowExecutionStatus
          (ExecutionStatus.valueOf(jobStatusIterator.next().getEventName()), latestFlowExecutionStatus);
    }

    return latestFlowExecutionStatus;
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
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().updateFlowConfig(flowId, flowConfig);
  }

  /**
   * Delete a configured flow. Running flows are not affected. The schedule will be removed for scheduled flows.
   * @param key composite key containing flow group and flow name that identifies the flow to remove from the flow catalog
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().deleteFlowConfig(flowId, getHeaders());
  }

  private FlowConfigsResourceHandler getFlowConfigResourceHandler() {
    if (global_flowConfigsResourceHandler != null) {
      return global_flowConfigsResourceHandler;
    }

    return flowConfigsResourceHandler;
  }

  private Properties getHeaders() {
    Properties headerProperties = new Properties();
    for (Map.Entry<String, String> entry : getContext().getRequestHeaders().entrySet()) {
      if (ALLOWED_METADATA.contains(entry.getKey())) {
        headerProperties.put(entry.getKey(), entry.getValue());
      }
    }
    return headerProperties;
  }
}

