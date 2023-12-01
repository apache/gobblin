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

import com.typesafe.config.Config;
import org.apache.gobblin.service.modules.dataset.ExternalUriDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;


/**
 * A DataNode for generic ingress/egress data movement outside of HDFS (HTTP or otherwise)
 */
public class ExternalUriDataNode extends BaseDataNode {
  public static final String EXTERNAL_PLATFORM_NAME = "external";

  public ExternalUriDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return EXTERNAL_PLATFORM_NAME;
  }

  @Override
  public String getDefaultDatasetDescriptorClass() {
    return ExternalUriDatasetDescriptor.class.getCanonicalName();
  }
}
