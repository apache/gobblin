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

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;


/**
 * An implementation of {@link LocalFSDataNode}. All the properties specific to a LocalFS based data node (e.g. fs.uri)
 * are validated here.
 */
@Alpha
public class LocalFSDataNode extends FileSystemDataNode {
  public static final String LOCAL_FS_SCHEME = "file";
  public static final String PLATFORM = "local";

  public LocalFSDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  /**
   *
   * @param fsUri FileSystem URI
   * @return true if the scheme of fsUri equals "file"
   */
  @Override
  public boolean isUriValid(URI fsUri) {
    String scheme = fsUri.getScheme();
    if (scheme.equals(LOCAL_FS_SCHEME)) {
      return true;
    }
    return false;
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return PLATFORM;
  }
}
