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


/**
 * An implementation of an ADL (Azure Data Lake) {@link org.apache.gobblin.service.modules.flowgraph.DataNode}.
 */
public class AdlsDataNode extends FileSystemDataNode {
  public static final String ADLS_SCHEME = "adl";
  public static final String ABFS_SCHEME = "abfs";

  public AdlsDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  /**
    * @param fsUri FileSystem URI
    * @return true if the scheme is "adl" and authority is not empty.
    */
  @Override
  public boolean isUriValid(URI fsUri) {
    String scheme = fsUri.getScheme();
    //Check that the scheme is "adl"
    if (!scheme.equals(ADLS_SCHEME) && !scheme.equals(ABFS_SCHEME)) {
      return false;
    }
    //Ensure that the authority is not empty
    if (Strings.isNullOrEmpty(fsUri.getAuthority())) {
      return false;
    }
    return true;
  }
}
