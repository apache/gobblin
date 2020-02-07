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

import org.apache.gobblin.service.modules.dataset.SqlDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

@EqualsAndHashCode (callSuper = true)
public class SqlDataNode extends BaseDataNode {
  public static final String SQL_HOSTNAME = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "sql.hostname";
  public static final String SQL_PORT = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "sql.port";
  public static final String SQL_DRIVER = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "sql.driver";

  @Getter
  private String hostName;
  @Getter
  private Integer port;
  @Getter
  private String jdbcDriver;

  public SqlDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
    try {
      this.hostName = ConfigUtils.getString(nodeProps, SQL_HOSTNAME, "");
      this.port = ConfigUtils.getInt(nodeProps, SQL_PORT, 0);
      this.jdbcDriver = ConfigUtils.getString(nodeProps, SQL_DRIVER, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(hostName), SQL_HOSTNAME + " cannot be null or empty.");
      Preconditions.checkArgument(port != 0, SQL_PORT + " cannot be empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(jdbcDriver), SQL_DRIVER + " cannot be null or empty.");
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
  }

  @Override
  public String getDefaultDatasetDescriptorClass() {
    return SqlDatasetDescriptor.class.getCanonicalName();
  }
}
