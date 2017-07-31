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
package org.apache.gobblin.compliance;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;

import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.hive.HiveSerDeWrapper;


/**
 * A class to represent Hive Partition Dataset object used for gobblin-compliance
 *
 * @author adsharma
 */
public class HivePartitionDataset implements Dataset {
  private Partition hivePartition;

  public HivePartitionDataset(Partition partition) {
    this.hivePartition = partition;
  }

  public HivePartitionDataset(HivePartitionDataset hivePartitionDataset) {
    this.hivePartition = hivePartitionDataset.hivePartition;
  }

  /**
   * Will return complete partition name i.e. dbName@tableName@partitionName
   */
  @Override
  public String datasetURN() {
    return this.hivePartition.getCompleteName();
  }

  public Path getLocation() {
    return this.hivePartition.getDataLocation();
  }

  public Path getTableLocation() {
    return this.hivePartition.getTable().getDataLocation();
  }

  public String getTableName() {
    return this.hivePartition.getTable().getTableName();
  }

  public String getDbName() {
    return this.hivePartition.getTable().getDbName();
  }

  public String getName() {
    return this.hivePartition.getName();
  }

  public Map<String, String> getSpec() {
    return this.hivePartition.getSpec();
  }

  public Map<String, String> getTableParams() {
    return this.hivePartition.getTable().getParameters();
  }

  public Map<String, String> getParams() {
    return this.hivePartition.getParameters();
  }

  public Properties getTableMetadata() {
    return this.hivePartition.getTable().getMetadata();
  }

  public List<FieldSchema> getCols() {
    return this.hivePartition.getTable().getCols();
  }

  public Optional<String> getFileFormat() {
    String serdeLib = this.hivePartition.getTPartition().getSd().getSerdeInfo().getSerializationLib();
    for (HiveSerDeWrapper.BuiltInHiveSerDe hiveSerDe : HiveSerDeWrapper.BuiltInHiveSerDe.values()) {
      if (hiveSerDe.toString().equalsIgnoreCase(serdeLib)) {
        return Optional.fromNullable(hiveSerDe.name());
      }
    }
    return Optional.<String>absent();
  }

  /**
   * @return the owner of the corresponding hive table
   */
  public Optional<String> getOwner() {
    return Optional.fromNullable(this.hivePartition.getTable().getOwner());
  }
}
