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

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

import joptsimple.internal.Strings;
import lombok.Getter;


/**
 * An abstract {@link FileSystemDataNode} implementation. In addition to the required properties of a {@link BaseDataNode}, an {@link FileSystemDataNode}
 * must have a FS URI specified. Example implementations of {@link FileSystemDataNode} include {@link HdfsDataNode}, {@link LocalFSDataNode}.
 */
@Alpha
public abstract class FileSystemDataNode extends BaseDataNode {
  public static final String FS_URI_KEY = "fs.uri";
  public static final String SOURCE_FS_URI_KEY = FlowGraphConfigurationKeys.DATA_NODE_SOURCE_PREFIX + "." + FS_URI_KEY;
  public static final String DESTINATION_FS_URI_KEY = FlowGraphConfigurationKeys.DATA_NODE_DESTINATION_PREFIX + "." + FS_URI_KEY;

  @Getter
  private String srcFsUri;
  @Getter
  private String destFsUri;

  /**
   * Constructor. An HDFS DataNode must have fs.uri property specified in addition to a node Id.
   */
  public FileSystemDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
    try {
      this.srcFsUri = ConfigUtils.getString(this.getSrcConfig(), SOURCE_FS_URI_KEY, "");
      this.destFsUri = ConfigUtils.getString(this.getDestConfig(), DESTINATION_FS_URI_KEY, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(this.srcFsUri) || !Strings.isNullOrEmpty(this.destFsUri),
          "Both src/dest fs.uri cannot be null or empty.");

      //Validate the srcFsUri and destFsUri of the DataNode.
      if (!Strings.isNullOrEmpty(this.srcFsUri) && !isUriValid(new URI(this.srcFsUri))) {
        throw new IOException("Invalid FS URI " + this.srcFsUri);
      }
      if (!Strings.isNullOrEmpty(this.destFsUri) && !isUriValid(new URI(this.destFsUri))) {
        throw new IOException("Invalid FS URI " + this.destFsUri);
      }
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
  }

  public abstract boolean isUriValid(URI fsUri);
  /**
   * Two HDFS DataNodes are the same if they have the same id and the same fsUri.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileSystemDataNode that = (FileSystemDataNode) o;

    return this.getId().equals(that.getId()) && srcFsUri.equals(that.getSrcFsUri()) && destFsUri.equals(that.getDestFsUri());
  }

  @Override
  public int hashCode() {
    return Joiner.on("-").join(this.getId(), this.srcFsUri, this.destFsUri).hashCode();
  }
}