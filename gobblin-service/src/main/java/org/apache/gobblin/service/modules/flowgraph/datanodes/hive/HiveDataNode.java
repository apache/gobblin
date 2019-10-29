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

package org.apache.gobblin.service.modules.flowgraph.datanodes.hive;

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import joptsimple.internal.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An abstract {@link HiveDataNode} implementation. In addition to the required properties of a {@link BaseDataNode}, an {@link HiveDataNode}
 * must have a metastore URI specified.
 */
@Alpha
@EqualsAndHashCode (callSuper = true)
public abstract class HiveDataNode extends BaseDataNode {
  public static final String METASTORE_URI_KEY = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri";

  @Getter
  private String metastoreUri;

  /**
   * Constructor. An HDFS DataNode must have fs.uri property specified in addition to a node Id.
   */
  public HiveDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
    try {
      this.metastoreUri = ConfigUtils.getString(nodeProps, METASTORE_URI_KEY, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(this.metastoreUri), "hive.metastore.uri cannot be null or empty.");

      //Validate the srcFsUri and destFsUri of the DataNode.
      if (!isUriValid(new URI(this.metastoreUri))) {
        throw new IOException("Invalid FS URI " + this.metastoreUri);
      }
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
  }

  public boolean isUriValid(URI fsUri) {
    // e.g. thrift://ltx1-holdemhcat01.grid.linkedin.com:7552
    return true;
  }
}