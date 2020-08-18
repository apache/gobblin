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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import joptsimple.internal.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link DataNode}.
 */
@Alpha
@Slf4j
@EqualsAndHashCode (exclude = {"rawConfig", "resolvedConfig", "active"})
public class BaseDataNode implements DataNode {
  @Getter
  private String id;
  @Getter
  private Config rawConfig;
  @Getter
  private Config resolvedConfig;
  @Getter
  private boolean active = true;

  public BaseDataNode(Config nodeProps) throws DataNodeCreationException {
    try {
      this.rawConfig = nodeProps;
      this.resolvedConfig = nodeProps.resolve();
      String nodeId = ConfigUtils.getString(this.resolvedConfig, FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(nodeId), "Node Id cannot be null or empty");
      this.id = nodeId;
      if (this.resolvedConfig.hasPath(FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY)) {
        this.active = this.resolvedConfig.getBoolean(FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY);
      }
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
  }

  @Override
  public String getDefaultDatasetDescriptorClass() {
    return null;
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return null;
  }
}
