/*
 *
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
  
  public static final String SUCCESS_POSTFIX = "Successful";
  public static final String FAILED_POSTFIX = "Failed";
  
  public static final String DB_NAME = "DBName";
  public static final String TABLE_NAME = "TableName";
  public static final String ERROR_MESSAGE = "ErrorMessage";

  // Path Registration
  protected static void submitSuccessfulPathRegistration(EventSubmitter eventSubmitter, HiveSpec spec) {
    eventSubmitter.submit("PathRegistration" + SUCCESS_POSTFIX,
        getAdditionalMetadata(spec, Optional.<Exception> absent()));
  }

  protected static void submitFailedPathRegistration(EventSubmitter eventSubmitter, HiveSpec spec, Exception error) {
    eventSubmitter.submit("PathRegistration" + FAILED_POSTFIX,
        getAdditionalMetadata(spec, Optional.<Exception> of(error)));
  }

  private static Map<String, String> getAdditionalMetadata(HiveSpec spec,
      Optional<Exception> error) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String> builder().put(DB_NAME, spec.getTable().getDbName())
            .put(TABLE_NAME, spec.getTable().getTableName()).put("Path", spec.getPath().toString());

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
    eventSubmitter.submit("DBCreation"+SUCCESS_POSTFIX, ImmutableMap.of(DB_NAME, dbName));
  }

  protected static void submitFailedDBCreation(EventSubmitter eventSubmitter, String dbName, Exception error) {
    eventSubmitter.submit("DBCreation"+FAILED_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(ERROR_MESSAGE, error.getMessage()).build());
  }

  // Table Creation
  protected static void submitSuccessfulTableCreation(EventSubmitter eventSubmitter, HiveTable table) {
    eventSubmitter.submit("TableCreation"+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> absent()));
  }

  protected static void submitFailedTableCreation(EventSubmitter eventSubmitter, HiveTable table, Exception error) {
    eventSubmitter.submit("TableCreation"+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> of(error)));
  }

  // Add partition
  protected static void submitSuccessfulPartitionAdd(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition) {
    eventSubmitter.submit("PartitionAdd"+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> absent()));
  }

  protected static void submitFailedPartitionAdd(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition, Exception error) {
    eventSubmitter.submit("PartitionAdd"+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> of(error)));
  }

  // Drop Table
  protected static void submitSuccessfulTableDrop(EventSubmitter eventSubmitter, String dbName, String tableName) {
    eventSubmitter.submit("TableDrop"+SUCCESS_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(TABLE_NAME, tableName).build());
  }

  protected static void submitFailedTableDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      Exception e) {
    eventSubmitter.submit("TableDrop"+FAILED_POSTFIX, ImmutableMap.<String, String> builder().put(DB_NAME, dbName)
        .put(TABLE_NAME, tableName).put(ERROR_MESSAGE, e.getMessage()).build());
  }

  // Drop partition
  protected static void submitSuccessfulPartitionDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      List<String> partitionValues) {
    eventSubmitter.submit("PartitionDrop"+SUCCESS_POSTFIX, ImmutableMap.<String, String> builder().put(DB_NAME, dbName)
        .put(TABLE_NAME, tableName).put("PartitionValues", partitionValues.toString()).build());
  }

  protected static void submitFailedPartitionDrop(EventSubmitter eventSubmitter, String dbName, String tableName,
      List<String> partitionValues, Exception error) {
    eventSubmitter.submit("PartitionDrop"+FAILED_POSTFIX,
        ImmutableMap.<String, String> builder().put(DB_NAME, dbName).put(TABLE_NAME, tableName)
            .put("PartitionValues", partitionValues.toString()).put(ERROR_MESSAGE, error.getMessage()).build());
  }

  // Alter Table
  protected static void submitSuccessfulTableAlter(EventSubmitter eventSubmitter, HiveTable table) {
    eventSubmitter.submit("TableAlter"+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> absent()));
  }

  protected static void submitFailedTableAlter(EventSubmitter eventSubmitter, HiveTable table, Exception error) {
    eventSubmitter.submit("TableAlter"+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> absent(), Optional.<Exception> of(error)));
  }

  // Alter partition
  protected static void submitSuccessfulPartitionAlter(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition) {
    eventSubmitter.submit("PartitionAlter"+SUCCESS_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> absent()));
  }

  protected static void submitFailedPartitionAlter(EventSubmitter eventSubmitter, HiveTable table,
      HivePartition partition, Exception error) {
    eventSubmitter.submit("PartitionAlter"+FAILED_POSTFIX, getAdditionalMetadata(table,
        Optional.<HivePartition> of(partition), Optional.<Exception> of(error)));
  }
}
