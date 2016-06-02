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
package gobblin.data.management.convertion.hive;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


/**
 * Builds the Hive avro to Orc conversion query. The record type for this converter is {@link QueryBasedHiveConversionEntity}. A {@link QueryBasedHiveConversionEntity}
 * can be a hive table or a hive partition.
 */
@Slf4j
public class HiveAvroToOrcConverter
    extends Converter<Schema, Schema, QueryBasedHiveConversionEntity, QueryBasedHiveConversionEntity> {

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Populate the avro to orc conversion query which will be available at
   * {@link QueryBasedHiveConversionEntity#getConversionQuery()}
   *
   */
  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema outputSchema,
      QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit) throws DataConversionException {
    conversionEntity.appendQuery("Insert into table ").appendQuery(conversionEntity.getHiveUnit().getDbName())
        .appendQuery(".").appendQuery(conversionEntity.getHiveUnit().getTableName()).appendQuery("_orc ")
        .appendQuery("select * from ").appendQuery(conversionEntity.getHiveUnit().getDbName()).appendQuery(".")
        .appendQuery(conversionEntity.getHiveUnit().getTableName());

    log.info("Conversion Query " + conversionEntity.getConversionQuery());
    return new SingleRecordIterable<>(conversionEntity);
  }

}
