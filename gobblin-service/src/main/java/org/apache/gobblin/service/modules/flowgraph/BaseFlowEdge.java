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

package org.apache.gobblin.service.modules.flowgraph;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import joptsimple.internal.Strings;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link FlowEdge}. If a {@link FSFlowTemplateCatalog} is specified in the constructor,
 * {@link #flowTemplate} is reloaded when {@link #getFlowTemplate()} is called.
 */
@Alpha
@Slf4j
public class BaseFlowEdge implements FlowEdge {
  @Getter
  protected String src;

  @Getter
  protected String dest;

  protected FlowTemplate flowTemplate;

  @Getter
  private List<SpecExecutor> executors;

  @Getter
  private Config config;

  @Getter
  private String id;

  @Getter
  private boolean active;

  private final FSFlowTemplateCatalog flowTemplateCatalog;

  //Constructor
  public BaseFlowEdge(List<String> endPoints, String edgeId, FlowTemplate flowTemplate, List<SpecExecutor> executors,
      Config properties, boolean active) {
    this(endPoints, edgeId, flowTemplate, executors, properties, active, null);
  }

  public BaseFlowEdge(List<String> endPoints, String edgeId, FlowTemplate flowTemplate, List<SpecExecutor> executors,
      Config properties, boolean active, FSFlowTemplateCatalog flowTemplateCatalog) {
    this.src = endPoints.get(0);
    this.dest = endPoints.get(1);
    this.flowTemplate = flowTemplate;
    this.executors = executors;
    this.active = active;
    this.config = properties;
    this.id = edgeId;
    this.flowTemplateCatalog = flowTemplateCatalog;
  }

  @Override
  public FlowTemplate getFlowTemplate() {
    try {
      if (this.flowTemplateCatalog != null) {
        this.flowTemplate = this.flowTemplateCatalog.getFlowTemplate(this.flowTemplate.getUri());
      }
    } catch (Exception e) {
      // If loading template fails, use the template that was successfully loaded on construction
      log.warn("Failed to get flow template " + this.flowTemplate.getUri() + ", using in-memory flow template", e);
    }
    return this.flowTemplate;
  }

  @Override
  public boolean isAccessible(UserGroupInformation user) {
    return true;
  }

  /**
   *   The {@link FlowEdge}s are the same if they have the same endpoints and both refer to the same {@FlowTemplate} i.e.
   *   the {@link FlowTemplate} uris are the same
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FlowEdge that = (FlowEdge) o;

    if (!(this.getSrc().equals(that.getSrc())) && ((this.getDest()).equals(that.getDest()))) {
      return false;
    }

    if (!this.getFlowTemplate().getUri().equals(that.getFlowTemplate().getUri())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return this.id.hashCode();
  }

  @Override
  public String toString() {
    return this.id;
  }

  /**
   * A {@link FlowEdgeFactory} for creating {@link BaseFlowEdge}.
   */
  public static class Factory implements FlowEdgeFactory {

    /**
     * A method to return an instance of {@link BaseFlowEdge}. The method performs all the validation checks
     * and returns
     * @param edgeProps Properties of edge
     * @param flowTemplateCatalog Flow Catalog used to retrieve {@link FlowTemplate}s.
     * @return a {@link BaseFlowEdge}
     */
    @Override
    public FlowEdge createFlowEdge(Config edgeProps, FSFlowTemplateCatalog flowTemplateCatalog, List<SpecExecutor> specExecutors)
        throws FlowEdgeCreationException {
      try {
        String source = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY, "");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(source), "A FlowEdge must have a non-null or empty source");
        String destination = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY, "");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destination), "A FlowEdge must have a non-null or empty destination");
        List<String> endPoints = Lists.newArrayList(source, destination);
        String edgeId = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(edgeId), "A FlowEdge must have a non-null or empty Id");

        String flowTemplateDirUri = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_DIR_URI_KEY, "");

        //Perform basic validation
        Preconditions.checkArgument(endPoints.size() == 2, "A FlowEdge must have 2 end points");
        Preconditions
            .checkArgument(specExecutors.size() > 0, "A FlowEdge must have at least one SpecExecutor");
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(flowTemplateDirUri), "FlowTemplate URI must be not null or empty");
        boolean isActive = ConfigUtils.getBoolean(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_IS_ACTIVE_KEY, true);

        FlowTemplate flowTemplate = flowTemplateCatalog.getFlowTemplate(new URI(flowTemplateDirUri));
        return new BaseFlowEdge(endPoints, edgeId, flowTemplate, specExecutors, edgeProps, isActive, flowTemplateCatalog);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new FlowEdgeCreationException(e);
      }
    }
  }
}
