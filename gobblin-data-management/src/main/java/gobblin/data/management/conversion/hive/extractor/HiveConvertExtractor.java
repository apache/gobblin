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

import gobblin.data.management.conversion.hive.HiveSource;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.provider.HdfsBasedSchemaProvider;
import gobblin.data.management.conversion.hive.provider.HiveAvroSchemaProvider;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * <p>
 * Extracts {@link QueryBasedHiveConversionEntity}s. A {@link QueryBasedHiveConversionEntity} can represent a
 * hive table or a hive partition. Note that this extractor does not extract rows of a partition or a table. Entire
 * table or partition is considered as a record.
 * </p>
 * <p>
 * This extractor deserializes the {@link HiveTable} or {@link HivePartition} serialized by the {@link HiveSource}
 * at {@link HiveSource#HIVE_UNIT_SERIALIZED_KEY} to build a {@link QueryBasedHiveConversionEntity}. Uses a {@link HiveAvroSchemaProvider}
 * to get the avro {@link Schema} of the {@link HiveTable} or {@link HivePartition} being extracted.
 * </p>
 */
public class HiveConvertExtractor implements Extractor<Schema, QueryBasedHiveConversionEntity> {

  private static final String OPTIONAL_HIVE_AVRO_SCHEMA_PROVIDER_CLASS_KEY = "hive.unit.avroSchemaProvider.class";
  private static final String DEFAULT_HIVE_AVRO_SCHEMA_PROVIDER_CLASS = HdfsBasedSchemaProvider.class.getName();

  /**
   * Even though each extractor only processes one {@link HiveRegistrationUnit}, We use a list with one {@link HiveRegistrationUnit}
   * so that {@link #readRecord(QueryBasedHiveConversionEntity)} knows when to stop.
   */
  private final List<HiveRegistrationUnit> hiveUnits;
  private final HiveAvroSchemaProvider hiveAvroSchemaProvider;

  public HiveConvertExtractor(WorkUnitState state, FileSystem fs) {
    HiveRegistrationUnit hiveUnit =
        HiveSource.GENERICS_AWARE_GSON.fromJson(state.getProp(HiveSource.HIVE_UNIT_SERIALIZED_KEY),
            HiveRegistrationUnit.class);

    this.hiveUnits = Lists.newArrayList(hiveUnit);
    this.hiveAvroSchemaProvider =
        GobblinConstructorUtils.invokeConstructor(HiveAvroSchemaProvider.class,
            state.getProp(OPTIONAL_HIVE_AVRO_SCHEMA_PROVIDER_CLASS_KEY, DEFAULT_HIVE_AVRO_SCHEMA_PROVIDER_CLASS), fs);

  }

  @Override
  public Schema getSchema() throws IOException {
    return this.hiveAvroSchemaProvider.getSchema(this.hiveUnits.get(0));
  }


  /**
   * There is only one record ({@link QueryBasedHiveConversionEntity}) to be read. This {@link QueryBasedHiveConversionEntity} is
   * removed from {@link #hiveUnits} list after it is read. So when gobblin runtime calls this method the second time, it return a null
   */
  @Override
  public QueryBasedHiveConversionEntity readRecord(QueryBasedHiveConversionEntity reuse) throws DataRecordException,
      IOException {

    if (this.hiveUnits.isEmpty()) {
      return null;
    }
    HiveRegistrationUnit hiveUnit = this.hiveUnits.remove(0);
    return new QueryBasedHiveConversionEntity(hiveUnit, this.hiveAvroSchemaProvider.getSchema(hiveUnit));
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
