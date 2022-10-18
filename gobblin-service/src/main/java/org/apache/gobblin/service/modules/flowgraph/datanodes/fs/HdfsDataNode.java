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

import com.google.common.base.Strings;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;


/**
 * An implementation of {@link HdfsDataNode}. All the properties specific to a HDFS based data node (e.g. fs.uri)
 * are validated here.
 */
@Alpha
public class HdfsDataNode extends FileSystemDataNode {
  public static final String HDFS_SCHEME = "hdfs";

  public HdfsDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  /**
   *
   * @param fsUri FileSystem URI
   * @return true if the scheme is "hdfs" and authority is not empty.
   */
  @Override
  public boolean isUriValid(URI fsUri) {
    String scheme = fsUri.getScheme();
    //Check that the scheme is "hdfs"
    if (!scheme.equals(getDefaultDatasetDescriptorPlatform())) {
      return false;
    }
    //Ensure that the authority is not empty
    if (Strings.isNullOrEmpty(fsUri.getAuthority())) {
      return false;
    }
    return true;
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return HDFS_SCHEME;
  }
}
