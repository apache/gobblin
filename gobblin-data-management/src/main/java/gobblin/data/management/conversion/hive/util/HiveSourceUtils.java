/*
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
package gobblin.data.management.conversion.hive.util;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.configuration.State;
import gobblin.data.management.conversion.hive.AvroSchemaManager;
import gobblin.data.management.conversion.hive.HiveSource;
import gobblin.data.management.conversion.hive.entities.SerializableHivePartition;
import gobblin.data.management.conversion.hive.entities.SerializableHiveTable;
import gobblin.source.workunit.WorkUnit;

/**
 * Utilities to build {@link WorkUnit}s in the {@link HiveSource}
 */
public class HiveSourceUtils {

  private static final String HIVE_TABLE_SERIALIZED_KEY = "hive.table.serialized";
  private static final String HIVE_PARTITION_SERIALIZED_KEY = "hive.partition.serialized";

  public static boolean hasPartition(State state) {
    return state.contains(HIVE_PARTITION_SERIALIZED_KEY);
  }

  public static void serializeTable(State state, Table table, AvroSchemaManager avroSchemaManager) throws IOException {

    state.setProp(HIVE_TABLE_SERIALIZED_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(
        new SerializableHiveTable(table.getDbName(), table.getTableName(), avroSchemaManager.getSchemaUrl(table)),
        SerializableHiveTable.class));
  }

  public static void serializePartition(State state, Partition partition, AvroSchemaManager avroSchemaManager)
      throws IOException {

    state.setProp(HIVE_PARTITION_SERIALIZED_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(
        new SerializableHivePartition(partition.getTable().getDbName(), partition.getTable().getTableName(), partition
            .getName(), avroSchemaManager.getSchemaUrl(partition)), SerializableHivePartition.class));
  }

  public static SerializableHiveTable deserializeTable(State state) throws IOException {
    return HiveSource.GENERICS_AWARE_GSON.fromJson(state.getProp(HIVE_TABLE_SERIALIZED_KEY),
        SerializableHiveTable.class);
  }

  public static SerializableHivePartition deserializePartition(State state) throws IOException {

    return HiveSource.GENERICS_AWARE_GSON.fromJson(state.getProp(HIVE_PARTITION_SERIALIZED_KEY),
        SerializableHivePartition.class);
  }

}
