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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

/**
 * {@link IcebergDatasetDescriptor} is a dataset descriptor for an Iceberg-based table, independent of the type of Iceberg catalog
 * Fields {@link IcebergDatasetDescriptor#databaseName} and {@link IcebergDatasetDescriptor#tableName} are used to
 * identify an iceberg.
 */
@EqualsAndHashCode(callSuper = true)
public class IcebergDatasetDescriptor extends BaseDatasetDescriptor {
  protected static final String SEPARATION_CHAR = ";";
  protected final String databaseName;
  protected final String tableName;
  @Getter
  private final String path;

  /**
   * Constructor for {@link IcebergDatasetDescriptor}
   * @param config
   * @throws IOException
   */
  public IcebergDatasetDescriptor(Config config) throws IOException {
    super(config);
    if (!isPlatformValid()) {
      throw new IOException("Invalid platform specified for IcebergDatasetDescriptor: " + getPlatform());
    }
    // setting defaults to empty; later used to throw as IO Exception
    this.databaseName = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DATABASE_KEY, "");
    this.tableName = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.TABLE_KEY, "");
    if (this.databaseName.isEmpty() || this.tableName.isEmpty()) {
      throw new IOException("Invalid iceberg database or table name: " + this.databaseName + ":" + this.tableName);
    }
    this.path = fullyQualifiedTableName(this.databaseName, this.tableName);
  }

  protected boolean isPlatformValid() {
    return "iceberg".equalsIgnoreCase(getPlatform());
  }

  private String fullyQualifiedTableName(String databaseName, String tableName) {
    return Joiner.on(SEPARATION_CHAR).join(databaseName, tableName);
  }

  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    if (otherPath == null) {
      return false;
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      return false;
    }

    String otherDbName = parts.get(0);
    String otherTableName = parts.get(1);

    return this.databaseName.equals(otherDbName) && this.tableName.equals(otherTableName);
  }
}
