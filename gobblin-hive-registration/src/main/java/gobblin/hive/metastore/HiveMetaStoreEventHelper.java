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

package gobblin.hive.metastore;

import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;
import gobblin.hive.spec.HiveSpec;
import gobblin.metrics.event.EventSubmitter;


/**
 * Helper class to populate hive registration event in state.
 */
public class HiveMetaStoreEventHelper {

  public static final String SUCCESS_POSTFIX = "Succeed";
  public static final String FAILED_POSTFIX = "Failed";

  public static final String DB_NAME = "DBName";
  public static final String TABLE_NAME = "TableName";
  public static final String PARTITIONS = "Partitions";
  public static final String ERROR_MESSAGE = "ErrorMessage";
  public static final String PATH_REGISTRATION = "PathRegistration";
  public static final String DB_CREATION = "DBCreation";
  public static final String TABLE_CREATION = "TableCreation";
  public static final String TABLE_DROP = "TableDrop";
  public static final String TABLE_ALTER = "TableAlter";

  public static final String PARTITION_CREATION = "PartitionCreation";
  public static final String PARTITION_DROP = "PartitionDrop";
  public static final String PARTITION_ALTER = "PartitionAlter";


  // Path Registration
  protected static void submitSuccessfulPathRegistration(EventSubmitter eventSubmitter, HiveSpec spec) {
    eventSubmitter.submit(PATH_REGISTRATION + SUCCESS_POSTFIX,
        getAdditionalMetadata(spec, Optional.<Exception> absent()));
  }

  protected static void submitFailedPathRegistration(EventSubmitter eventSubmitter, HiveSpec spec, Exception error) {
    eventSubmitter.submit(PATH_REGISTRATION + FAILED_POSTFIX,
        getAdditionalMetadata(spec, Optional.<Exception> of(error)));
  }

  private static Map<String, String> getAdditionalMetadata(HiveSpec spec,
      Optional<Exception> error) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String> builder().put(DB_NAME, spec.getTable().getDbName())
            .put(TABLE_NAME, spec.getTable().getTableName()).put("Path", spec.getPath().toString());

    if(spec.getPartition().isPresent()){
      builder.put(PARTITIONS, spec.getPartition().get().toString());
    }

    if (error.isPresent()) {
      builder.put(ERROR_MESSAGE, error.get().getMessage());
    }

    return builder.build();
  }

  private static Map<String, String> getAdditionalMetadata(HiveTable table,
      Optional<HivePartition> partition, Optional<Exception> error) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String> builder().put(DB_NAME, table.getDbName()).put(TABLE_NAME, table.getTableName());

    if (table.getLocation().isPresent()) {
      builder.put("Location", table.getLocation().get());
    }

    if (partition.isPresent()) {
      builder.put("Partition", partition.get().toString());
    }

    if (error.isPresent()) {
      builder.put(ERROR_MESSAGE, error.get().getMessage());
    }

    return builder.build();
  }

  // DB Creation
  protected static void submitSuccessfulDBCreation(EventSubmitter eventSubmitter, String dbName) {
    eventSubmitter.submit(DB_CREATION+SUCCESS_POSTFIX, ImmutableMap.of(DB_NAME, dbName));
  }

  protected static void submitFailedDBCreation(EventSubmitter eventSubmitter, String dbName, Exception error) {
    eventSubmitter.submit(DB_CREATION+FAILED_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(ERROR_MESSAGE, error.getMessage()).build());
  }

  // Table Creation
  protected static void submitSuccessfulTableCreation(EventSubmitter eventSubmitter, HiveTable table) {
    eventSubmitter.submit(TABLE_CREATION+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> absent()));
  }

  protected static void submitFailedTableCreation(EventSubmitter eventSubmitter, HiveTable table, Exception error) {
    eventSubmitter.submit(TABLE_CREATION+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> of(error)));
  }

  // Add partition
  protected static void submitSuccessfulPartitionAdd(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition) {
    eventSubmitter.submit(PARTITION_CREATION+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> absent()));
  }

  protected static void submitFailedPartitionAdd(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition, Exception error) {
    eventSubmitter.submit(PARTITION_CREATION+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> of(error)));
  }

  // Drop Table
  protected static void submitSuccessfulTableDrop(EventSubmitter eventSubmitter, String dbName,
      String tableName, String metastoreURI) {
    eventSubmitter.submit(TABLE_DROP+SUCCESS_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(TABLE_NAME, tableName).
            put("metastoreURI", metastoreURI).build());
  }

  protected static void submitFailedTableDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      Exception e) {
    eventSubmitter.submit(TABLE_DROP+FAILED_POSTFIX, ImmutableMap.<String, String> builder().put(DB_NAME, dbName)
        .put(TABLE_NAME, tableName).put(ERROR_MESSAGE, e.getMessage()).build());
  }

  // Drop partition
  protected static void submitSuccessfulPartitionDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      List<String> partitionValues, String metastoreURI) {
    eventSubmitter.submit(PARTITION_DROP+SUCCESS_POSTFIX, ImmutableMap.<String, String> builder().put(DB_NAME, dbName)
        .put(TABLE_NAME, tableName).put("PartitionValues", partitionValues.toString())
        .put("metastoreURI", metastoreURI).build());
  }

  protected static void submitFailedPartitionDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      List<String> partitionValues, Exception error) {
    eventSubmitter.submit(PARTITION_DROP+FAILED_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(TABLE_NAME, tableName)
            .put("PartitionValues", partitionValues.toString()).put(ERROR_MESSAGE, error.getMessage()).build());
  }

  // Alter Table
  protected static void submitSuccessfulTableAlter(EventSubmitter eventSubmitter, HiveTable table) {
    eventSubmitter.submit(TABLE_ALTER+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> absent()));
  }

  protected static void submitFailedTableAlter(EventSubmitter eventSubmitter, HiveTable table, Exception error) {
    eventSubmitter.submit(TABLE_ALTER+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> of(error)));
  }

  // Alter partition
  protected static void submitSuccessfulPartitionAlter(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition) {
    eventSubmitter.submit(PARTITION_ALTER+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> absent()));
  }

  protected static void submitFailedPartitionAlter(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition, Exception error) {
    eventSubmitter.submit(PARTITION_ALTER+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> of(error)));
  }
}
