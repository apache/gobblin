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

package org.apache.gobblin.iceberg.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metadata.IntegerBytesPair;
import org.apache.gobblin.metadata.IntegerLongPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;


@Slf4j
public class IcebergUtils {

  private static final String AVRO_SCHEMA_URL = "avro.schema.url";
  private static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
  private static final String[] RESTRICTED_PROPERTIES =
      new String[]{AVRO_SCHEMA_URL, AVRO_SCHEMA_LITERAL};

  private IcebergUtils() {
  }
  /**
   * Calculate the {@Link PartitionSpec} used to create iceberg table
   */
  public static PartitionSpec getPartitionSpec(Schema tableSchema, Schema partitionSchema) {
    //TODO: Add more information into partition spec e.g. day, year, month, kafka partition ids, offset ranges for better consuming
    PartitionSpec.Builder builder = PartitionSpec.builderFor(tableSchema);
    partitionSchema.asStruct().fields().forEach(f -> builder.identity(f.name()));
    return builder.build();
  }

  /**
   * Given a avro schema string and a hive table,
   * calculate the iceberg table schema and partition schema.
   * (E.g. we use 'datepartition' as the partition column, which is not included inside the data schema,
   * we'll need to add that column to data schema to construct table schema
   */
  public static IcebergDataAndPartitionSchema getIcebergSchema(String schema,
      org.apache.hadoop.hive.metastore.api.Table table) {

    org.apache.iceberg.shaded.org.apache.avro.Schema icebergDataSchema =
        new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(schema);
    Types.StructType dataStructType = AvroSchemaUtil.convert(icebergDataSchema).asStructType();
    List<Types.NestedField> dataFields = Lists.newArrayList(dataStructType.fields());
    org.apache.iceberg.shaded.org.apache.avro.Schema icebergPartitionSchema =
        parseSchemaFromCols(table.getPartitionKeys(), table.getDbName(), table.getTableName(), true);
    Types.StructType partitionStructType = AvroSchemaUtil.convert(icebergPartitionSchema).asStructType();
    List<Types.NestedField> partitionFields = partitionStructType.fields();
    Preconditions.checkArgument(partitionFields.stream().allMatch(f -> f.type().isPrimitiveType()),
        "Only primitive fields are supported for partition columns");
    dataFields.addAll(partitionFields);
    Types.StructType updatedStructType = Types.StructType.of(dataFields);
    updatedStructType =
        (Types.StructType) TypeUtil.assignFreshIds(updatedStructType, new AtomicInteger(0)::incrementAndGet);
    return new IcebergDataAndPartitionSchema(new org.apache.iceberg.Schema(updatedStructType.fields()),
        new org.apache.iceberg.Schema(partitionFields));
  }

  private static org.apache.iceberg.shaded.org.apache.avro.Schema parseSchemaFromCols(List<FieldSchema> cols,
      String namespace, String recordName, boolean mkFieldsOptional) {
    final List<String> colNames = new ArrayList<>(cols.size());
    final List<TypeInfo> colsTypeInfo = new ArrayList<>(cols.size());
    cols.forEach(fs -> {
      colNames.add(fs.getName());
      colsTypeInfo.add(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
    });
    final TypeInfoToSchemaParser parser =
        new TypeInfoToSchemaParser(namespace, mkFieldsOptional, Collections.emptyMap());
    return new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(
        parser.parseSchemaFromFieldsTypeInfo("", recordName, colNames, colsTypeInfo).toString());
  }

  /**
   * Given a Hive table, get all the properties of the table, and drop unneeded ones and transfer to a map
   */
  public static Map<String, String> getTableProperties(org.apache.hadoop.hive.metastore.api.Table table) {
    final Map<String, String> parameters = getRawTableProperties(table);
    // drop unneeded parameters
    for (String k : RESTRICTED_PROPERTIES) {
      parameters.remove(k);
    }
    return parameters;
  }

  private static Map<String, String> getRawTableProperties(org.apache.hadoop.hive.metastore.api.Table table) {
    final Map<String, String> parameters = new HashMap<>();
    // lowest to highest priority of updating tableProperties
    parameters.putAll(table.getSd().getSerdeInfo().getParameters());
    parameters.putAll(table.getSd().getParameters());
    parameters.putAll(table.getParameters());
    return parameters;
  }

  /**
   * Get the iceberg partition value for given partition strings
   */
  public static StructLike getPartition(Types.StructType partitionType, List<String> partitionValues) {
    //TODO parse partitionValue as per partitionSchema
    return new StructLike() {
      @Override
      public int size() {
        return partitionValues.size();
      }

      @Override
      public <T> T get(int pos, Class<T> javaClass) {
        return partitionValue(partitionType.fields().get(pos), partitionValues.get(pos));
      }

      @Override
      public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static <T> T partitionValue(Types.NestedField partitionField, String colAsString) {
    Preconditions.checkState(partitionField.type().isPrimitiveType(), "Partition column {} is not of primitive type",
        partitionField);
    return (T) Conversions.fromPartitionString(partitionField.type(), colAsString);
  }

  /**
   * Transfer list of {@Link IntegerLongPair} from origin id to long, to Map<Integer, Long> from real column id to long
   * This method is mainly used to get parse the file metrics from GMCE
   * @param list list of {@Link IntegerLongPair}
   * @param schemaIdMap A map from origin ID (defined by data pipeline) to the real iceberg table column id
   * @return A map from real id to long as the file metrics
   */
  public static Map<Integer, Long> getMapFromIntegerLongPairs(
      List<IntegerLongPair> list, Map<Integer, Integer> schemaIdMap) {
    //If schemaIdMap is not set, we directly return null to avoid set wrong file metrics
    if (list == null || list.size() == 0 || schemaIdMap == null) {
      return null;
    }
    try {
      return list.stream().filter(t -> schemaIdMap.containsKey(t.getKey()))
          .collect(Collectors.toMap(t -> schemaIdMap.get(t.getKey()), IntegerLongPair::getValue));
    } catch (Exception e) {
      log.warn("get exception {} when calculate metrics", e);
      return null;
    }
  }

  /**
   * Transfer list of {@Link IntegerBytesPair} from origin id to bytes, to Map<Integer, ByteBuffer> from real column id to ByteBuffer
   * This method is mainly used to get parse the file metrics from GMCE
   * @param list list of {@Link IntegerBytesPair} from origin id to bytes
   * @param schemaIdMap A map from origin ID (defined by data pipeline) to the real iceberg table column id
   * @return A map from real id to ByteBuffer as the file metrics
   */
  public static Map<Integer, ByteBuffer> getMapFromIntegerBytesPairs(
      List<IntegerBytesPair> list, Map<Integer, Integer> schemaIdMap) {
    //If schemaWithOriginId is not set, we directly return null to avoid set wrong file metrics
    if (list == null || list.size() == 0 || schemaIdMap == null) {
      return null;
    }
    try {
      return list.stream().filter(t -> schemaIdMap.containsKey(t.getKey()))
          .collect(Collectors.toMap(t -> schemaIdMap.get(t.getKey()), IntegerBytesPair::getValue));
    } catch (Exception e) {
      log.warn("get exception {} when calculate metrics", e);
      return null;
    }
  }

  /**
   * Method to get DataFile without format and metrics information
   * This method is mainly used to get the file to be deleted
   */
  public static DataFile getIcebergDataFileWithoutMetric(String file, PartitionSpec partitionSpec,
      StructLike partitionVal) {
    //Use raw Path to support federation.
    String rawPath = new Path(file).toUri().getRawPath();
    //Just want to remove the old files, so set the record number and file size to a random value
    DataFiles.Builder dataFileBuilder =
        DataFiles.builder(partitionSpec).withPath(rawPath).withFileSizeInBytes(0).withRecordCount(0);

    if (partitionVal != null) {
      dataFileBuilder.withPartition(partitionVal);
    }
    return dataFileBuilder.build();
  }

  /**
   * Method to get DataFile with format and metrics information
   * This method is mainly used to get the file to be added
   */
  public static DataFile getIcebergDataFileWithMetric(org.apache.gobblin.metadata.DataFile file,
      PartitionSpec partitionSpec, StructLike partition, Configuration conf, Map<Integer, Integer> schemaIdMap) {
    Path filePath = new Path(file.getFilePath());
    DataFiles.Builder dataFileBuilder = DataFiles.builder(partitionSpec);
    try {
      // Use absolute path to support federation
      dataFileBuilder.withPath(filePath.toUri().getRawPath())
          .withFileSizeInBytes(filePath.getFileSystem(conf).getFileStatus(filePath).getLen())
          .withFormat(file.getFileFormat());
    } catch (IOException exception) {
      throw new RuntimeIOException(exception, "Failed to get dataFile for path: %s", filePath);
    }
    if (partition != null) {
      dataFileBuilder.withPartition(partition);
    }
    Metrics metrics = new Metrics(file.getFileMetrics().getRecordCount(),
        IcebergUtils.getMapFromIntegerLongPairs(file.getFileMetrics().getColumnSizes(), schemaIdMap),
        IcebergUtils.getMapFromIntegerLongPairs(file.getFileMetrics().getValueCounts(), schemaIdMap),
        IcebergUtils.getMapFromIntegerLongPairs(file.getFileMetrics().getNullValueCounts(), schemaIdMap),
        IcebergUtils.getMapFromIntegerBytesPairs(file.getFileMetrics().getLowerBounds(), schemaIdMap),
        IcebergUtils.getMapFromIntegerBytesPairs(file.getFileMetrics().getUpperBounds(), schemaIdMap));
    return dataFileBuilder.withMetrics(metrics).build();
  }

  /**
   * Calculate the schema id map from origin id (file metrics used) to the current table schema id
   * @param schemaWithOriginId  coming from GMCE, contains the original schema id which is used to calculate the file metrics
   * @param tableSchema table schema, coming from iceberg
   * @return Schema id map
   */
  public static Map<Integer, Integer> getSchemaIdMap(Schema schemaWithOriginId, Schema tableSchema) {
    if (schemaWithOriginId == null || tableSchema == null) {
      return null;
    }
    Map<Integer, Integer> map = new HashMap();
    Map<String, Integer> originNameIdMap = TypeUtil.indexByName(schemaWithOriginId.asStruct());
    for (Map.Entry<String, Integer> nameIdPair : originNameIdMap.entrySet()) {
      if (tableSchema.findField(nameIdPair.getKey()) != null) {
        map.put(nameIdPair.getValue(), tableSchema.findField(nameIdPair.getKey()).fieldId());
      } else {
        log.warn("Cannot find field {}, will skip the metrics for this column", nameIdPair.getKey());
      }
    }
    return map;
  }

  /**
   * Method to get Iceberg format from state, the format is determined by {@Link ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY}
   */
  public static FileFormat getIcebergFormat(State state) {
    if (state.getProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY).equalsIgnoreCase("AVRO")) {
      return FileFormat.AVRO;
    } else if (state.getProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY).equalsIgnoreCase("ORC")) {
      return FileFormat.ORC;
    }

    throw new IllegalArgumentException("Unsupported data format: " + state.getProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY));
  }


  public static class IcebergDataAndPartitionSchema {
    public Schema tableSchema;
    public Schema partitionSchema;

    IcebergDataAndPartitionSchema(Schema tableSchema, Schema partitionSchema) {
      this.tableSchema = tableSchema;
      this.partitionSchema = partitionSchema;
    }
  }
}
