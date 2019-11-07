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

package org.apache.gobblin.service.modules.flowgraph.datanodes;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import joptsimple.internal.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Represents a HTTP source. Whether the source provides a REST complied API is not enforced.
 * Currently supports HTTPS with port default to 443
 */
@EqualsAndHashCode (callSuper = true)
public class HttpDataNode extends BaseDataNode  {

  @Getter
  private String httpDomain;
  @Getter
  private String authenticationType;

  public HttpDataNode(Config nodeProps) throws DataNode.DataNodeCreationException {
    super(nodeProps);
    try {
      this.httpDomain = ConfigUtils.getString(nodeProps, FlowGraphConfigurationKeys.DATA_NODE_HTTP_DOMAIN_KEY, "");
      // Authentication details and credentials should reside in the Gobblin job payload
      this.authenticationType = ConfigUtils.getString(
          nodeProps, FlowGraphConfigurationKeys.DATA_NODE_HTTP_AUTHENTICATION_TYPE_KEY, "");

      Preconditions.checkArgument(!Strings.isNullOrEmpty(httpDomain),
          FlowGraphConfigurationKeys.DATA_NODE_HTTP_DOMAIN_KEY + " cannot be null or empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(authenticationType),
          FlowGraphConfigurationKeys.DATA_NODE_HTTP_AUTHENTICATION_TYPE_KEY + " cannot be null or empty.");
    } catch (Exception e) {
      throw new DataNode.DataNodeCreationException(e);
    }
  }
}
