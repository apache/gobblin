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

package gobblin.hive.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveConstants;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveRegistrationUnit.Column;
import gobblin.hive.HiveTable;


/**
 * A utility class for converting Hive's {@link Table} and {@link Partition} objects into Gobblin's
 * {@link HiveTable} and {@link HivePartition} objects, and vice versa.
 *
 * @author Ziyang Liu
 */
@Alpha
public class HiveMetaStoreUtils {

  private static final TableType DEFAULT_TABLE_TYPE = TableType.EXTERNAL_TABLE;
  private static final String EXTERNAL = "EXTERNAL";

  private HiveMetaStoreUtils() {}

  /**
   * Convert a {@link HiveTable} into a {@link Table}.
   */
  public static Table getTable(HiveTable hiveTable) {
    State props = hiveTable.getProps();
    Table table = new Table();
    table.setDbName(hiveTable.getDbName());
    table.setTableName(hiveTable.getTableName());
    table.setParameters(getParameters(props));
    if (hiveTable.getCreateTime().isPresent()) {
      table.setCreateTime(Ints.checkedCast(hiveTable.getCreateTime().get()));
    }
    if (hiveTable.getLastAccessTime().isPresent()) {
      table.setLastAccessTime(Ints.checkedCast(hiveTable.getLastAccessTime().get()));
    }
    if (hiveTable.getOwner().isPresent()) {
      table.setOwner(hiveTable.getOwner().get());
    }
    if (hiveTable.getRetention().isPresent()) {
      table.setRetention(Ints.checkedCast(hiveTable.getRetention().get()));
    }
    if (hiveTable.getTableType().isPresent()) {
      table.setTableType(hiveTable.getTableType().get());
    } else {
      table.setTableType(DEFAULT_TABLE_TYPE.toString());
    }
    if (table.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
      table.getParameters().put(EXTERNAL, Boolean.TRUE.toString().toUpperCase());
    }
    table.setPartitionKeys(getFieldSchemas(hiveTable.getPartitionKeys()));
    table.setSd(getStorageDescriptor(hiveTable));
    return table;
  }

  /**
   * Convert a {@link Table} into a {@link HiveTable}.
   */
  public static HiveTable getHiveTable(Table table) {
    State tableProps = getTableProps(table);
    State storageProps = getStorageProps(table.getSd());
    State serDeProps = getSerDeProps(table.getSd().getSerdeInfo());
    HiveTable hiveTable = new HiveTable.Builder().withDbName(table.getDbName()).withTableName(table.getTableName())
        .withPartitionKeys(getColumns(table.getPartitionKeys())).withProps(tableProps).withStorageProps(storageProps)
        .withSerdeProps(serDeProps).build();
    if (table.getSd().getCols() != null) {
      hiveTable.setColumns(getColumns(table.getSd().getCols()));
    }
    if (table.getSd().getBucketCols() != null) {
      hiveTable.setBucketColumns(table.getSd().getBucketCols());
    }
    return hiveTable;
  }

  /**
   * Convert a {@link HivePartition} into a {@link Partition}.
   */
  public static Partition getPartition(HivePartition hivePartition) {
    State props = hivePartition.getProps();
    Partition partition = new Partition();
    partition.setDbName(hivePartition.getDbName());
    partition.setTableName(hivePartition.getTableName());
    partition.setValues(hivePartition.getValues());
    partition.setParameters(getParameters(props));
    if (props.contains(HiveConstants.CREATE_TIME)) {
      partition.setCreateTime(props.getPropAsInt(HiveConstants.CREATE_TIME));
    }
    if (props.contains(HiveConstants.LAST_ACCESS_TIME)) {
      partition.setLastAccessTime(props.getPropAsInt(HiveConstants.LAST_ACCESS_TIME));
    }
    partition.setSd(getStorageDescriptor(hivePartition));
    return partition;
  }

  /**
   * Convert a {@link Partition} into a {@link HivePartition}.
   */
  public static HivePartition getHivePartition(Partition partition) {
    State partitionProps = getPartitionProps(partition);
    State storageProps = getStorageProps(partition.getSd());
    State serDeProps = getSerDeProps(partition.getSd().getSerdeInfo());
    HivePartition hivePartition = new HivePartition.Builder().withDbName(partition.getDbName())
        .withTableName(partition.getTableName()).withPartitionValues(partition.getValues()).withProps(partitionProps)
        .withStorageProps(storageProps).withSerdeProps(serDeProps).build();
    if (partition.getSd().getCols() != null) {
      hivePartition.setColumns(getColumns(partition.getSd().getCols()));
    }
    if (partition.getSd().getBucketCols() != null) {
      hivePartition.setBucketColumns(partition.getSd().getBucketCols());
    }
    return hivePartition;
  }

  private static Map<String, String> getParameters(State props) {
    Map<String, String> parameters = Maps.newHashMap();
    for (String propKey : props.getPropertyNames()) {
      parameters.put(propKey, props.getProp(propKey));
    }
    return parameters;
  }

  private static StorageDescriptor getStorageDescriptor(HiveRegistrationUnit unit) {
    State props = unit.getStorageProps();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setParameters(getParameters(props));
    sd.setCols(getFieldSchemas(unit.getColumns()));
    if (unit.getLocation().isPresent()) {
      sd.setLocation(unit.getLocation().get());
    }
    if (unit.getInputFormat().isPresent()) {
      sd.setInputFormat(unit.getInputFormat().get());
    }
    if (unit.getOutputFormat().isPresent()) {
      sd.setOutputFormat(unit.getOutputFormat().get());
    }
    if (unit.getIsCompressed().isPresent()) {
      sd.setCompressed(unit.getIsCompressed().get());
    }
    if (unit.getNumBuckets().isPresent()) {
      sd.setNumBuckets(unit.getNumBuckets().get());
    }
    if (unit.getBucketColumns().isPresent()) {
      sd.setBucketCols(unit.getBucketColumns().get());
    }
    if (unit.getIsStoredAsSubDirs().isPresent()) {
      sd.setStoredAsSubDirectories(unit.getIsStoredAsSubDirs().get());
    }
    sd.setSerdeInfo(getSerDeInfo(unit));
    return sd;
  }

  private static SerDeInfo getSerDeInfo(HiveRegistrationUnit unit) {
    State props = unit.getSerDeProps();
    SerDeInfo si = new SerDeInfo();
    si.setParameters(getParameters(props));
    si.setName(unit.getTableName());
    if (unit.getSerDeType().isPresent()) {
      si.setSerializationLib(unit.getSerDeType().get());
    }
    return si;
  }

  private static State getTableProps(Table table) {
    State tableProps = new State();
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      tableProps.setProp(entry.getKey(), entry.getValue());
    }
    if (table.isSetCreateTime()) {
      tableProps.setProp(HiveConstants.CREATE_TIME, table.getCreateTime());
    }
    if (table.isSetLastAccessTime()) {
      tableProps.setProp(HiveConstants.LAST_ACCESS_TIME, table.getCreateTime());
    }
    if (table.isSetOwner()) {
      tableProps.setProp(HiveConstants.OWNER, table.getOwner());
    }
    if (table.isSetTableType()) {
      tableProps.setProp(HiveConstants.TABLE_TYPE, table.getTableType());
    }
    if (table.isSetRetention()) {
      tableProps.setProp(HiveConstants.RETENTION, table.getRetention());
    }
    return tableProps;
  }

  private static State getPartitionProps(Partition partition) {
    State partitionProps = new State();
    for (Map.Entry<String, String> entry : partition.getParameters().entrySet()) {
      partitionProps.setProp(entry.getKey(), entry.getValue());
    }
    if (partition.isSetCreateTime()) {
      partitionProps.setProp(HiveConstants.CREATE_TIME, partition.getCreateTime());
    }
    if (partition.isSetLastAccessTime()) {
      partitionProps.setProp(HiveConstants.LAST_ACCESS_TIME, partition.getCreateTime());
    }
    return partitionProps;
  }

  private static State getStorageProps(StorageDescriptor sd) {
    State storageProps = new State();
    for (Map.Entry<String, String> entry : sd.getParameters().entrySet()) {
      storageProps.setProp(entry.getKey(), entry.getValue());
    }
    if (sd.isSetLocation()) {
      storageProps.setProp(HiveConstants.LOCATION, sd.getLocation());
    }
    if (sd.isSetInputFormat()) {
      storageProps.setProp(HiveConstants.INPUT_FORMAT, sd.getInputFormat());
    }
    if (sd.isSetOutputFormat()) {
      storageProps.setProp(HiveConstants.OUTPUT_FORMAT, sd.getOutputFormat());
    }
    if (sd.isSetCompressed()) {
      storageProps.setProp(HiveConstants.COMPRESSED, sd.isCompressed());
    }
    if (sd.isSetNumBuckets()) {
      storageProps.setProp(HiveConstants.NUM_BUCKETS, sd.getNumBuckets());
    }
    if (sd.isSetBucketCols()) {
      for (String bucketColumn : sd.getBucketCols())
        storageProps.appendToListProp(HiveConstants.BUCKET_COLUMNS, bucketColumn);
    }
    if (sd.isSetStoredAsSubDirectories()) {
      storageProps.setProp(HiveConstants.STORED_AS_SUB_DIRS, sd.isStoredAsSubDirectories());
    }
    return storageProps;
  }

  private static State getSerDeProps(SerDeInfo si) {
    State serDeProps = new State();
    for (Map.Entry<String, String> entry : si.getParameters().entrySet()) {
      serDeProps.setProp(entry.getKey(), entry.getValue());
    }
    if (si.isSetSerializationLib()) {
      serDeProps.setProp(HiveConstants.SERDE_TYPE, si.getSerializationLib());
    }
    return serDeProps;
  }

  private static List<Column> getColumns(List<FieldSchema> fieldSchemas) {
    List<Column> columns = Lists.newArrayListWithCapacity(fieldSchemas.size());
    for (FieldSchema fieldSchema : fieldSchemas) {
      columns.add(new Column(fieldSchema.getName(), fieldSchema.getType(), fieldSchema.getComment()));
    }
    return columns;
  }

  private static List<FieldSchema> getFieldSchemas(List<Column> columns) {
    List<FieldSchema> fieldSchemas = Lists.newArrayListWithCapacity(columns.size());
    for (Column column : columns) {
      fieldSchemas.add(new FieldSchema(column.getName(), column.getType(), column.getComment()));
    }
    return fieldSchemas;
  }

}
