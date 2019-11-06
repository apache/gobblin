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

import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.util.ConfigUtils;

/**
 * As of now, {@link HiveDatasetDescriptor} has same implementation as that of {@link SqlDatasetDescriptor}.
 * It can have partitioning related information in the future.
 */
@EqualsAndHashCode (callSuper = true)
public class HiveDatasetDescriptor extends SqlDatasetDescriptor {
  static final String IS_PARTITIONED_KEY = "isPartitioned";
  static final String PARTITION_COLUMN = "partition.column";
  static final String PARTITION_FORMAT = "partition.format";
  private final boolean isPartitioned;
  private final String partitionColumn;
  private final String partitionFormat;

  public HiveDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.isPartitioned = config.getBoolean(IS_PARTITIONED_KEY);

    if (isPartitioned) {
      partitionColumn = ConfigUtils.getString(config, PARTITION_COLUMN, "datepartition");
      partitionFormat = ConfigUtils.getString(config, PARTITION_FORMAT, "YYYY-MM-dd-HH");
    } else {
      partitionColumn = "";
      partitionFormat = "";
    }
  }

  @Override
  protected boolean isPlatformValid() {
    return "hive".equalsIgnoreCase(getPlatform());
  }

  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    return super.isPathContaining(other) && this.isPartitioned == ((HiveDatasetDescriptor) other).isPartitioned;
  }
}
