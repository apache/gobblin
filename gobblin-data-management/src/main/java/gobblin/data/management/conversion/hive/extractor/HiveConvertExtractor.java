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
package gobblin.data.management.conversion.hive.extractor;

import java.io.IOException;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.avro.AvroSchemaManager;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;
import gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import gobblin.source.extractor.DataRecordException;
import gobblin.util.AutoReturnableObject;


/**
 * <p>
 * Extracts {@link QueryBasedHiveConversionEntity}s. A {@link QueryBasedHiveConversionEntity} can represent a
 * hive table or a hive partition. Note that this extractor does not extract rows of a partition or a table. Entire
 * table or partition is considered as a record.
 * </p>
 * <p>
 * From the {@link WorkUnitState} this extractor deserializes the {@link SerializableHiveTable} and optionally a {@link SerializableHivePartition}.
 * For these {@link SerializableHiveTable} and {@link SerializableHivePartition}'s the extractor makes a call to the Hive metastore
 * to get the corresponding hive {@link org.apache.hadoop.hive.ql.metadata.Table} and hive {@link org.apache.hadoop.hive.ql.metadata.Partition}
 * </p>
 */
@Slf4j
public class HiveConvertExtractor extends HiveBaseExtractor<Schema, QueryBasedHiveConversionEntity> {

  private List<QueryBasedHiveConversionEntity> conversionEntities = Lists.newArrayList();

  public HiveConvertExtractor(WorkUnitState state, FileSystem fs) throws IOException, TException, HiveException {
    super(state);

    if (Boolean.valueOf(state.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      log.info("Ignoring Watermark workunit for {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY));
      return;
    }

    if (!(this.hiveDataset instanceof ConvertibleHiveDataset)) {
      throw new IllegalStateException("HiveConvertExtractor is only compatible with ConvertibleHiveDataset");
    }

    ConvertibleHiveDataset convertibleHiveDataset = (ConvertibleHiveDataset) this.hiveDataset;

    try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {
      Table table = client.get().getTable(this.dbName, this.tableName);

      SchemaAwareHiveTable schemaAwareHiveTable = new SchemaAwareHiveTable(table, AvroSchemaManager.getSchemaFromUrl(this.hiveWorkUnit.getTableSchemaUrl(), fs));

      SchemaAwareHivePartition schemaAwareHivePartition = null;

      if (this.hiveWorkUnit.getPartitionName().isPresent() && this.hiveWorkUnit.getPartitionSchemaUrl().isPresent()) {

        Partition partition = client.get().getPartition(this.dbName, this.tableName, this.hiveWorkUnit.getPartitionName().get());
        schemaAwareHivePartition =
            new SchemaAwareHivePartition(table, partition, AvroSchemaManager.getSchemaFromUrl(this.hiveWorkUnit.getPartitionSchemaUrl().get(), fs));
      }

      QueryBasedHiveConversionEntity entity =
          new QueryBasedHiveConversionEntity(convertibleHiveDataset, schemaAwareHiveTable, Optional.fromNullable(schemaAwareHivePartition));
      this.conversionEntities.add(entity);
    }

  }

  @Override
  public Schema getSchema() throws IOException {
    if (this.conversionEntities.isEmpty()) {
      return Schema.create(Type.NULL);
    }

    QueryBasedHiveConversionEntity conversionEntity = this.conversionEntities.get(0);
    return conversionEntity.getHiveTable().getAvroSchema();
  }

  /**
   * There is only one record ({@link QueryBasedHiveConversionEntity}) to be read. This {@link QueryBasedHiveConversionEntity} is
   * removed from {@link #conversionEntities} list after it is read. So when gobblin runtime calls this method the second time, it returns a null
   */
  @Override
  public QueryBasedHiveConversionEntity readRecord(QueryBasedHiveConversionEntity reuse) throws DataRecordException, IOException {

    if (this.conversionEntities.isEmpty()) {
      return null;
    }

    return this.conversionEntities.remove(0);
  }
}
