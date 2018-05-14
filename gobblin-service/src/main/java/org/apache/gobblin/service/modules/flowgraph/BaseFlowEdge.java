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

import java.util.List;
import java.util.Properties;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.util.ConfigUtils;


import lombok.Getter;

/**
 * An implementation of {@link FlowEdge}.
 */
@Alpha
public class BaseFlowEdge implements FlowEdge {
  @Getter
  protected List<String> endPoints;

  @Getter
  protected FlowTemplate flowTemplate;

  @Getter
  private List<SpecExecutor> executors;

  @Getter
  private Config props;

  @Getter
  private boolean active;

  //Constructor
  public BaseFlowEdge(List<String> endPoints, FlowTemplate flowTemplate, List<SpecExecutor> executors, Config properties, boolean active) {
    this.endPoints = endPoints;
    this.flowTemplate = flowTemplate;
    this.executors = executors;
    this.active = active;
    this.props = properties;
  }

  @Override
  public boolean isAccessible(UserGroupInformation user) {
    return true;
  }

  /**
   *   The FlowEdges are the same if they have the same endpoints and both refer to the same {@FlowTemplate} i.e.
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
    return (Joiner.on(":").join(this.getEndPoints().get(0), this.getEndPoints().get(1), this.getFlowTemplate().getUri())).hashCode();
  }

  @Override
  public String toString() {
    return Joiner.on(":").join(this.getEndPoints().get(0), this.getEndPoints().get(1), this.getFlowTemplate().getUri().getPath());
  }

  public static class Builder {
    private List<String> endPoints;
    private FlowTemplate template;
    private List<SpecExecutor> executors;
    private boolean active;
    private Config properties;

    public Builder withEndPoints(String sourceNode, String destNode) {
        this.endPoints = Lists.newArrayList(sourceNode,destNode);
        return this;
    }

    public Builder withFlowTemplate(FlowTemplate template) {
      this.template = template;
      return this;
    }

    public Builder withExecutors(List<SpecExecutor> executors) {
      this.executors = executors;
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.properties = ConfigUtils.propertiesToConfig(properties);
      return this;
    }

    public Builder setActive(boolean active) {
      this.active = active;
      return this;
    }

    public BaseFlowEdge build() {
      return new BaseFlowEdge(endPoints, template, executors, properties, active);
    }
  }
}
