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
package gobblin.data.management.conversion.hive.extractor;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.AvroSchemaManager;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;
import gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
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
public class HiveConvertExtractor implements Extractor<Schema, QueryBasedHiveConversionEntity> {

  private List<QueryBasedHiveConversionEntity> conversionEntities = Lists.newArrayList();

  public HiveConvertExtractor(WorkUnitState state, FileSystem fs) throws IOException, TException, HiveException {

    HiveWorkUnit hiveWokUnit = new HiveWorkUnit(state.getWorkunit());

    if (!(hiveWokUnit.getHiveDataset() instanceof ConvertibleHiveDataset)) {
      throw new IllegalStateException("HiveConvertExtractor is only compatible with ConvertibleHiveDataset");
    }

    ConvertibleHiveDataset hiveDataset = (ConvertibleHiveDataset) hiveWokUnit.getHiveDataset();
    String dbName = hiveDataset.getDbAndTable().getDb();
    String tableName = hiveDataset.getDbAndTable().getTable();

    HiveMetastoreClientPool pool =
        HiveMetastoreClientPool.get(state.getJobState().getProperties(),
            Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
    try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
      Table table = client.get().getTable(dbName, tableName);

      SchemaAwareHiveTable schemaAwareHiveTable = new SchemaAwareHiveTable(table, AvroSchemaManager.getSchemaFromUrl(hiveWokUnit.getTableSchemaUrl(), fs));

      SchemaAwareHivePartition schemaAwareHivePartition = null;

      if (hiveWokUnit.getPartitionName().isPresent() && hiveWokUnit.getPartitionSchemaUrl().isPresent()) {

        Partition partition = client.get().getPartition(dbName, tableName, hiveWokUnit.getPartitionName().get());
        schemaAwareHivePartition =
            new SchemaAwareHivePartition(table, partition, AvroSchemaManager.getSchemaFromUrl(hiveWokUnit.getPartitionSchemaUrl().get(), fs));
      }

      QueryBasedHiveConversionEntity entity =
          new QueryBasedHiveConversionEntity(hiveDataset, schemaAwareHiveTable, Optional.fromNullable(schemaAwareHivePartition));
      this.conversionEntities.add(entity);
    }

  }

  @Override
  public Schema getSchema() throws IOException {
    if (this.conversionEntities.isEmpty()) {
      return null;
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

  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  /**
   * Watermark is not managed by this extractor.
   */
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close() throws IOException {
  }

}
