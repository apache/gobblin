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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

/**
 * Finder class for locating and creating partitioned Iceberg datasets.
 * <p>
 * This class extends {@link IcebergDatasetFinder} and provides functionality to create
 * {@link IcebergPartitionDataset} instances based on the specified source and destination Iceberg catalogs.
 * </p>
 */
@Slf4j
public class IcebergPartitionDatasetFinder extends IcebergDatasetFinder {
  public static final String ICEBERG_PARTITION_NAME_KEY = "partition.name";
  public static final String ICEBERG_PARTITION_VALUE_KEY = "partition.value";
  public static final String ICEBERG_PARTITION_DATASET_PARTITION_SPEC_STRICT_EQUALITY = ICEBERG_DATASET_PREFIX + "strictEqualityForPartitionSpec";
  // Taking default value as true so that no partition spec evaluation is allowed on neither source nor destination
  public static final String DEFAULT_ICEBERG_PARTITION_DATASET_PARTITION_SPEC_STRICT_EQUALITY = "true";

  public IcebergPartitionDatasetFinder(FileSystem sourceFs, Properties properties) {
    super(sourceFs, properties);
  }

  @Override
  protected IcebergDataset createSpecificDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable,
      Properties properties, FileSystem fs, boolean shouldIncludeMetadataPath) throws IOException {

    boolean strictEqualityForPartitionSpec = Boolean.parseBoolean(properties.getProperty(ICEBERG_PARTITION_DATASET_PARTITION_SPEC_STRICT_EQUALITY,
        DEFAULT_ICEBERG_PARTITION_DATASET_PARTITION_SPEC_STRICT_EQUALITY));

    IcebergTableMetadataValidatorUtils.failUnlessCompatibleStructure(
        srcIcebergTable.accessTableMetadata(), destIcebergTable.accessTableMetadata(), strictEqualityForPartitionSpec);

    String partitionColumnName = getLocationQualifiedProperty(properties, IcebergDatasetFinder.CatalogLocation.SOURCE,
        ICEBERG_PARTITION_NAME_KEY);
    Preconditions.checkArgument(StringUtils.isNotEmpty(partitionColumnName),
        "Partition column name cannot be empty");

    String partitionColumnValue = getLocationQualifiedProperty(properties, IcebergDatasetFinder.CatalogLocation.SOURCE,
        ICEBERG_PARTITION_VALUE_KEY);
    Preconditions.checkArgument(StringUtils.isNotEmpty(partitionColumnValue),
        "Partition value cannot be empty");

    return new IcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, fs,
        getConfigShouldCopyMetadataPath(properties), partitionColumnName, partitionColumnValue);
  }
}