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

import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.dataset.HiveDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;


/**
 * An {@link HiveDataNode} implementation. In addition to the required properties of a {@link BaseDataNode}, an {@link HiveDataNode}
 * specifies platform as hive.
 */
@Alpha
@EqualsAndHashCode (callSuper = true)
public class HiveDataNode extends HiveMetastoreUriDataNode {
  public static final String PLATFORM = "hive";

  /**
   * Constructor. A HiveDataNode must have hive.metastore.uri property specified in addition to a node Id and fs.uri.
   */
  public HiveDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  @Override
  public String getDefaultDatasetDescriptorClass() {
    return HiveDatasetDescriptor.class.getCanonicalName();
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return PLATFORM;
  }
}