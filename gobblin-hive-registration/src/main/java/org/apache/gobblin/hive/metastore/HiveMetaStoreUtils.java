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

package org.apache.gobblin.hive.metastore;

import com.google.common.base.Splitter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.SchemaParseException;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveConstants;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveRegistrationUnit.Column;
import org.apache.gobblin.hive.HiveTable;


/**
 * A utility class for converting Hive's {@link Table} and {@link Partition} objects into Gobblin's
 * {@link HiveTable} and {@link HivePartition} objects, and vice versa.
 *
 * @author Ziyang Liu
 */
@Alpha
public class HiveMetaStoreUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreUtils.class);

  private static final TableType DEFAULT_TABLE_TYPE = TableType.EXTERNAL_TABLE;
  private static final Splitter LIST_SPLITTER_COMMA = Splitter.on(",").trimResults().omitEmptyStrings();
  private static final Splitter LIST_SPLITTER_COLON = Splitter.on(":").trimResults().omitEmptyStrings();
  private static final String EXTERNAL = "EXTERNAL";
  public static final String RUNTIME_PROPS = "runtime.props";

  private HiveMetaStoreUtils() {
  }

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
    if (table.getCreateTime() > 0) {
      hiveTable.setCreateTime(table.getCreateTime());
    }
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
    if (hivePartition.getCreateTime().isPresent()) {
      partition.setCreateTime(Ints.checkedCast(hivePartition.getCreateTime().get()));
    } else if (props.contains(HiveConstants.CREATE_TIME)) {
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
    HivePartition hivePartition =
        new HivePartition.Builder().withDbName(partition.getDbName()).withTableName(partition.getTableName())
            .withPartitionValues(partition.getValues()).withProps(partitionProps).withStorageProps(storageProps)
            .withSerdeProps(serDeProps).build();
    if (partition.getCreateTime() > 0) {
      hivePartition.setCreateTime(partition.getCreateTime());
    }
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
    if (props.contains(RUNTIME_PROPS)) {
      String runtimePropsString = props.getProp(RUNTIME_PROPS);
      for (String propValue : LIST_SPLITTER_COMMA.splitToList(runtimePropsString)) {
        List<String> tokens = LIST_SPLITTER_COLON.splitToList(propValue);
        Preconditions.checkState(tokens.size() == 2,
            propValue + " is not a valid Hive table/partition property");
        parameters.put(tokens.get(0), tokens.get(1));
      }
    }
    for (String propKey : props.getPropertyNames()) {
      if (!propKey.equals(RUNTIME_PROPS)) {
        parameters.put(propKey, props.getProp(propKey));
      }
    }
    return parameters;
  }

  private static StorageDescriptor getStorageDescriptor(HiveRegistrationUnit unit) {
    State props = unit.getStorageProps();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setParameters(getParameters(props));
    sd.setCols(getFieldSchemas(unit));
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
      for (String bucketColumn : sd.getBucketCols()) {
        storageProps.appendToListProp(HiveConstants.BUCKET_COLUMNS, bucketColumn);
      }
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

  /**
   * First tries getting the {@code FieldSchema}s from the {@code HiveRegistrationUnit}'s columns, if set.
   * Else, gets the {@code FieldSchema}s from the deserializer.
   */
  private static List<FieldSchema> getFieldSchemas(HiveRegistrationUnit unit) {
    List<Column> columns = unit.getColumns();
    List<FieldSchema> fieldSchemas = new ArrayList<>();
    if (columns != null && columns.size() > 0) {
      fieldSchemas = getFieldSchemas(columns);
    } else {
      Deserializer deserializer = getDeserializer(unit);
      if (deserializer != null) {
        try {
          fieldSchemas = MetaStoreUtils.getFieldsFromDeserializer(unit.getTableName(), deserializer);
        } catch (SerDeException | MetaException e) {
          LOG.warn("Encountered exception while getting fields from deserializer.", e);
        }
      }
    }
    return fieldSchemas;
  }

  /**
   * Returns a Deserializer from HiveRegistrationUnit if present and successfully initialized. Else returns null.
   */
  private static Deserializer getDeserializer(HiveRegistrationUnit unit) {
    Optional<String> serdeClass = unit.getSerDeType();
    if (!serdeClass.isPresent()) {
      return null;
    }

    String serde = serdeClass.get();
    HiveConf hiveConf = new HiveConf();

    Deserializer deserializer;
    try {
      deserializer =
          ReflectionUtils.newInstance(hiveConf.getClassByName(serde).asSubclass(Deserializer.class), hiveConf);
    } catch (ClassNotFoundException e) {
      LOG.warn("Serde class " + serde + " not found!", e);
      return null;
    }

    Properties props = new Properties();
    props.putAll(unit.getProps().getProperties());
    props.putAll(unit.getStorageProps().getProperties());
    props.putAll(unit.getSerDeProps().getProperties());

    try {
      SerDeUtils.initializeSerDe(deserializer, hiveConf, props, null);

      // Temporary check that's needed until Gobblin is upgraded to Hive 1.1.0+, which includes the improved error
      // handling in AvroSerDe added in HIVE-7868.
      if (deserializer instanceof AvroSerDe) {
        try {
          inVokeDetermineSchemaOrThrowExceptionMethod(props, new Configuration());
        } catch (SchemaParseException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
          LOG.warn("Failed to initialize AvroSerDe.");
          throw new SerDeException(e);
        }
      }
    } catch (SerDeException e) {
      LOG.warn("Failed to initialize serde " + serde + " with properties " + props + " for table " + unit.getDbName() +
          "." + unit.getTableName());
      return null;
    }

    return deserializer;
  }

  @VisibleForTesting
  protected static void inVokeDetermineSchemaOrThrowExceptionMethod(Properties props, Configuration conf)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    String methodName = "determineSchemaOrThrowException";
    Method method = MethodUtils.getAccessibleMethod(AvroSerdeUtils.class, methodName, Properties.class);
    boolean withConf = false;
    if (method == null) {
      method = MethodUtils
          .getAccessibleMethod(AvroSerdeUtils.class, methodName, new Class[]{Configuration.class, Properties.class});
      withConf = true;
    }
    Preconditions.checkNotNull(method, "Cannot find matching " + methodName);
    if (!withConf) {
      MethodUtils.invokeStaticMethod(AvroSerdeUtils.class, methodName, props);
    } else {
      MethodUtils.invokeStaticMethod(AvroSerdeUtils.class, methodName, new Object[]{conf, props});
    }
  }
}
