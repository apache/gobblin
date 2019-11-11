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
import com.typesafe.config.ConfigValueFactory;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.version.finder.DatePartitionHiveVersionFinder;
import org.apache.gobblin.util.ConfigUtils;

/**
 * As of now, {@link HiveDatasetDescriptor} has same implementation as that of {@link SqlDatasetDescriptor}.
 * Fields {@link HiveDatasetDescriptor#isPartitioned}, {@link HiveDatasetDescriptor#partitionColumn} and
 * {@link HiveDatasetDescriptor#partitionFormat} are used for methods 'equals' and 'hashCode'.
 */
@EqualsAndHashCode (callSuper = true)
public class HiveDatasetDescriptor extends SqlDatasetDescriptor {
  static final String IS_PARTITIONED_KEY = "isPartitioned";
  static final String PARTITION_COLUMN = "partition.column";
  static final String PARTITION_FORMAT = "partition.format";
  static final String CONFLICT_POLICY = "conflict.policy";
  private final boolean isPartitioned;
  private final String partitionColumn;
  private final String partitionFormat;

  public HiveDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.isPartitioned = ConfigUtils.getBoolean(config, IS_PARTITIONED_KEY, true);

    if (isPartitioned) {
      partitionColumn = ConfigUtils.getString(config, PARTITION_COLUMN, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_KEY_NAME);
      partitionFormat = ConfigUtils.getString(config, PARTITION_FORMAT, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_VALUE_DATE_TIME_PATTERN);
      this.setRawConfig(this.getRawConfig().withValue(CONFLICT_POLICY,
          ConfigValueFactory.fromAnyRef(HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_PARTITIONS.name())));
      this.setRawConfig(this.getRawConfig().withValue(PARTITION_COLUMN, ConfigValueFactory.fromAnyRef(partitionColumn)));
      this.setRawConfig(this.getRawConfig().withValue(PARTITION_FORMAT, ConfigValueFactory.fromAnyRef(partitionFormat)));
    } else {
      partitionColumn = "";
      partitionFormat = "";
      this.setRawConfig(this.getRawConfig().withValue(CONFLICT_POLICY,
          ConfigValueFactory.fromAnyRef(HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_TABLE.name())));
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
