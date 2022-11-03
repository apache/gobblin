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

package org.apache.gobblin.service.modules.flowgraph.datanodes.iceberg;

import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog;
import org.apache.gobblin.service.modules.dataset.IcebergDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.datanodes.hive.HiveDataNode;

@Alpha
@EqualsAndHashCode(callSuper = true)
public class IcebergDataNode extends HiveDataNode {
  public static final String PLATFORM = "iceberg";
  /**
   * Constructor. A IcebergDataNode must have hive.metastore.uri property specified to get {@link IcebergHiveCatalog} information
   * @param nodeProps
   */
  public IcebergDataNode(Config nodeProps)
      throws DataNodeCreationException {
    super(nodeProps);
  }
  @Override
  public String getDefaultDatasetDescriptorClass() {
    return IcebergDatasetDescriptor.class.getCanonicalName();
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return PLATFORM;
  }

}
