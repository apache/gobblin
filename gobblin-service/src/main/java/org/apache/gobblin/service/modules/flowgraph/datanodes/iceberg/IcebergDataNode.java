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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import joptsimple.internal.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog;
import org.apache.gobblin.service.modules.dataset.IcebergDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Specifies iceberg platform and uniquely identifies an Iceberg Catalog based on the URI.
 * See {@link IcebergHiveCatalog} for more information
 */
@Alpha
@EqualsAndHashCode(callSuper = true)
public class IcebergDataNode extends BaseDataNode {
  public static final String CATALOG_URI_KEY = FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "iceberg.catalog.uri";
  public static final String PLATFORM = "iceberg";

  @Getter
  private String catalogUri;
  /**
   * Constructor. An IcebergDataNode must have iceberg.catalog.uri property specified to get information about specific catalog. eg. {@link IcebergHiveCatalog}
   * @param nodeProps
   */
  public IcebergDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
    try {
      this.catalogUri = ConfigUtils.getString(nodeProps, CATALOG_URI_KEY, "");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(this.catalogUri), "iceberg.catalog.uri cannot be null or empty.");
    } catch (Exception e) {
      throw new DataNodeCreationException(e);
    }
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
