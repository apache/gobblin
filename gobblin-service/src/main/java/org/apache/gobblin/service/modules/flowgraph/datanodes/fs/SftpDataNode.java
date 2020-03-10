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
package org.apache.gobblin.service.modules.flowgraph.datanodes.fs;

import java.net.URI;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

@EqualsAndHashCode(callSuper = true)
public class SftpDataNode extends FileSystemDataNode {
  public static final String SFTP_SCHEME = "sftp";
  public static final String DEFAULT_SFTP_URI = "sftp:///";
  public static final String PLATFORM = "sftpfs";
  public static final String SFTP_HOSTNAME = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "sftp.hostname";
  public static final String SFTP_PORT = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "sftp.port";

  @Getter
  private String hostName;
  @Getter
  private Integer port;
  @Getter
  private Config rawConfig;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(SFTP_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT)
          .build());

  /**
   * Constructor. A SFTP DataNode must have {@link SftpDataNode#SFTP_HOSTNAME} configured.
   */
  public SftpDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps.withFallback(ConfigFactory.empty().withValue(FileSystemDataNode.FS_URI_KEY, ConfigValueFactory.fromAnyRef(DEFAULT_SFTP_URI))));
    try {
      this.rawConfig = nodeProps.withFallback(DEFAULT_FALLBACK).withFallback(super.getRawConfig());
      this.hostName = ConfigUtils.getString(this.rawConfig, SFTP_HOSTNAME, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(hostName), SFTP_HOSTNAME + " cannot be null or empty.");
      this.port = ConfigUtils.getInt(this.rawConfig, SFTP_PORT, -1);
      Preconditions.checkArgument(this.port > 0,  "Invalid value for " + SFTP_PORT + ": " + this.port);
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
  }

  @Override
  public boolean isUriValid(URI fsUri) {
    return fsUri.getScheme().equals(SFTP_SCHEME);
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return PLATFORM;
  }
}
