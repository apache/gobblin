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
package gobblin.data.management.conversion.hive.source;

import java.lang.reflect.Type;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.source.workunit.WorkUnit;


/**
 * A {@link WorkUnit} wrapper for {@link HiveSource}. This is class is meant to hide the keys at which workunit values are stored.
 * The source class is supposed to read/write values into the {@link WorkUnit} through getters/setters instead of directly accessing
 * through {@link #getProp(String, String)}/{@link #setProp(String, Object)}
 */
public class HiveWorkUnit extends WorkUnit {

  private static final String PREFIX = "hive.source.dataset";
  private static final String HIVE_DATASET_SERIALIZED_KEY = PREFIX + ".serialized";
  private static final String HIVE_TABLE_SCHEMA_URL_KEY = PREFIX + ".table.schemaUrl";
  private static final String HIVE_TABLE_LOCATION_KEY = PREFIX + ".table.location";
  private static final String HIVE_PARTITION_SCHEMA_URL_KEY = PREFIX + ".partition.schemaUrl";
  private static final String HIVE_PARTITION_NAME_KEY = PREFIX + ".partition.name";
  private static final String HIVE_PARTITION_LOCATION_KEY = PREFIX + ".partition.location";
  private static final String HIVE_PARTITION_KEYS = PREFIX + ".partition.keys";

  private static final Gson GSON = new Gson();
  private static final Type FIELD_SCHEMA_TYPE = new TypeToken<List<FieldSchema>>() {}.getType();

  @SuppressWarnings("deprecation")
  public HiveWorkUnit() {
    super();
  }

  @SuppressWarnings("deprecation")
  public HiveWorkUnit(WorkUnit workunit) {
    super(workunit);
  }

  /**
   * Automatically serializes the {@link HiveDataset} by calling {@link #setHiveDataset(HiveDataset)}
   * @param hiveDataset for which the workunit is being created
   */
  @SuppressWarnings("deprecation")
  public HiveWorkUnit(HiveDataset hiveDataset) {
    super();
    setHiveDataset(hiveDataset);
  }

  /**
   * Sets the {@link ConfigurationKeys#DATASET_URN_KEY} key.
   */
  public void setDatasetUrn(String datasetUrn) {
    this.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);
  }

  public String getDatasetUrn(String datasetUrn) {
    return this.getProp(ConfigurationKeys.DATASET_URN_KEY);
  }

  /**
   * Automatically sets the dataset urn by calling {@link #setDatasetUrn(String)}
   */
  public void setHiveDataset(HiveDataset hiveDataset) {
    this.setProp(HIVE_DATASET_SERIALIZED_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(hiveDataset, HiveDataset.class));
    setDatasetUrn(hiveDataset.getTable().getCompleteName());
  }

  public HiveDataset getHiveDataset() {
    return HiveSource.GENERICS_AWARE_GSON.fromJson(this.getProp(HIVE_DATASET_SERIALIZED_KEY), HiveDataset.class);
  }

  /**
   * Set the schema url for this table into the {@link WorkUnit}
   */
  public void setTableSchemaUrl(Path schemaUrl) {
    this.setProp(HIVE_TABLE_SCHEMA_URL_KEY, schemaUrl.toString());
  }

  public Path getTableSchemaUrl() {
    return new Path(this.getProp(HIVE_TABLE_SCHEMA_URL_KEY));
  }

  /**
   * Set the schema url for a partition into the {@link WorkUnit}
   */
  public void setPartitionSchemaUrl(Path schemaUrl) {
    this.setProp(HIVE_PARTITION_SCHEMA_URL_KEY, schemaUrl.toString());
  }

  /**
   * Get the schema url path for the partition if this {@link WorkUnit} is for a partitioned table.
   * If not, return {@link Optional#absent()}
   */
  public Optional<Path> getPartitionSchemaUrl() {
    return StringUtils.isNotBlank(this.getProp(HIVE_PARTITION_SCHEMA_URL_KEY)) ? Optional.<Path> of(new Path(this.getProp(HIVE_PARTITION_SCHEMA_URL_KEY)))
        : Optional.<Path> absent();
  }

  /**
   * Set the name of the partition into the {@link WorkUnit}
   * @param partitionName
   */
  public void setPartitionName(String partitionName) {
    this.setProp(HIVE_PARTITION_NAME_KEY, partitionName);
  }

  /**
   * Get the name for the partition if this {@link WorkUnit} is for a partitioned table.
   * If not, return {@link Optional#absent()}
   */
  public Optional<String> getPartitionName() {
    return Optional.fromNullable(this.getProp(HIVE_PARTITION_NAME_KEY));
  }

  /**
   * Set the name of the partition into the {@link WorkUnit}
   * @param partitionName
   */
  public void setTableLocation(String partitionLocation) {
    this.setProp(HIVE_TABLE_LOCATION_KEY, partitionLocation);
  }

  /**
   * Get the name for the partition if this {@link WorkUnit} is for a partitioned table.
   * If not, return {@link Optional#absent()}
   */
  public Optional<String> getTableLocation() {
    return Optional.fromNullable(this.getProp(HIVE_TABLE_LOCATION_KEY));
  }


  /**
   * Set the name of the partition into the {@link WorkUnit}
   * @param partitionName
   */
  public void setPartitionLocation(String partitionLocation) {
    this.setProp(HIVE_PARTITION_LOCATION_KEY, partitionLocation);
  }

  /**
   * Get the name for the partition if this {@link WorkUnit} is for a partitioned table.
   * If not, return {@link Optional#absent()}
   */
  public Optional<String> getPartitionLocation() {
    return Optional.fromNullable(this.getProp(HIVE_PARTITION_LOCATION_KEY));
  }

  /**
   * Set partition keys into the {@link WorkUnit}
   * @param partitionName
   */
  public void setPartitionKeys(List<FieldSchema> partitionKeys) {
    this.setProp(HIVE_PARTITION_KEYS, GSON.toJson(partitionKeys, FIELD_SCHEMA_TYPE));
  }

  /**
   * Get the partition keys if this {@link WorkUnit} is for a partitioned table.
   * If not, return {@link Optional#absent()}
   */
  public Optional<List<FieldSchema>> getPartitionKeys() {
    String serialzed = this.getProp(HIVE_PARTITION_KEYS);
    if (serialzed == null) {
      return Optional.absent();
    }
    List<FieldSchema> deserialized = GSON.fromJson(serialzed, FIELD_SCHEMA_TYPE);
    return Optional.of(deserialized);
  }
}
