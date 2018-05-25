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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.util.ConfigUtils;

import joptsimple.internal.Strings;
import lombok.Getter;


/**
 * An implementation of {@link FlowEdge}.
 */
@Alpha
public class BaseFlowEdge implements FlowEdge {
  public static final String FLOW_EDGE_LABEL_JOINER_CHAR = ":";

  @Getter
  protected List<String> endPoints;

  @Getter
  protected FlowTemplate flowTemplate;

  @Getter
  private List<SpecExecutor> executors;

  @Getter
  private Config props;

  @Getter
  private String id;

  @Getter
  private boolean active;

  //Constructor
  public BaseFlowEdge(List<String> endPoints, String edgeName, FlowTemplate flowTemplate, List<SpecExecutor> executors, Config properties, boolean active) {
    this.endPoints = endPoints;
    this.flowTemplate = flowTemplate;
    this.executors = executors;
    this.active = active;
    this.props = properties;
    this.id = generateEdgeId(endPoints, edgeName);
  }

  @Override
  public boolean isAccessible(UserGroupInformation user) {
    return true;
  }

  @VisibleForTesting
  protected static String generateEdgeId(List<String> endPoints, String edgeName) {
    return Joiner.on(FLOW_EDGE_LABEL_JOINER_CHAR).join(endPoints.get(0), endPoints.get(1), edgeName);
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

    if(!(this.getEndPoints().get(0).equals(that.getEndPoints().get(0))) && ((this.getEndPoints().get(1)).equals(that.getEndPoints().get(1)))) {
      return false;
    }

    if(!this.getFlowTemplate().getUri().equals(that.getFlowTemplate().getUri())) {
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
     * @param flowCatalog Flow Catalog used to retrieve {@link FlowTemplate}s.
     * @return a {@link BaseFlowEdge}
     */
    @Override
    public FlowEdge createFlowEdge(Config edgeProps, FSFlowCatalog flowCatalog) throws FlowEdgeCreationException {
      try {
        String source = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY,"");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(source), "A FlowEdge must have a non-null or empty source");
        String destination = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY,"");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destination), "A FlowEdge must have a non-null or empty destination");
        List<String> endPoints = Lists.newArrayList(source, destination);
        String edgeName = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY,"");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(edgeName), "A FlowEdge must have a non-null or empty name");

        List<Config> specExecutorConfigList = new ArrayList<>();
        boolean flag;
        for(int i = 0; (flag = edgeProps.hasPath(FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + "." + i)) != false; i++) {
          specExecutorConfigList.add(edgeProps.getConfig(FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + "." + i));
        }

        String flowTemplateUri = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_URI_KEY, "");

        //Perform basic validation
        Preconditions.checkArgument(endPoints.size() == 2, "A FlowEdge must have 2 end points");
        Preconditions
            .checkArgument(specExecutorConfigList.size() > 0, "A FlowEdge must have at least one SpecExecutor");
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(flowTemplateUri), "FlowTemplate URI must be not null or empty");
        boolean isActive = ConfigUtils.getBoolean(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_IS_ACTIVE_KEY, true);

        //Build SpecExecutor from config
        List<SpecExecutor> specExecutors = new ArrayList<>();

        for (Config specExecutorConfig : specExecutorConfigList) {
          Class executorClass = Class.forName(specExecutorConfig.getString(FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY));
          SpecExecutor executor = (SpecExecutor) GobblinConstructorUtils.invokeLongestConstructor(executorClass, specExecutorConfig);
          specExecutors.add(executor);
        }
        FlowTemplate flowTemplate = flowCatalog.getFlowTemplate(new URI(flowTemplateUri));
        return new BaseFlowEdge(endPoints, edgeName, flowTemplate, specExecutors, edgeProps, isActive);
      } catch (Exception e) {
        throw new FlowEdgeCreationException(e);
      }
    }

    @Override
    public String getEdgeId(Config edgeProps) throws IOException {
      String source = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY,"");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(source), "A FlowEdge must have a non-null or empty source");
      String destination = ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY,"");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(source), "A FlowEdge must have a non-null or empty destination");
      String edgeName =
          ConfigUtils.getString(edgeProps, FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(edgeName), "A FlowEdge must have a non-null or empty name");
      List<String> endPoints = Lists.newArrayList(source, destination);

      return generateEdgeId(endPoints, edgeName);
    }
  }
}
