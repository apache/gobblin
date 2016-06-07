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
package gobblin.data.management.conversion.hive.entities;

import gobblin.data.management.conversion.hive.converter.HiveAvroToOrcConverter;
import gobblin.data.management.conversion.hive.writer.HiveQueryExecutionWriter;
import gobblin.data.management.conversion.hive.extractor.HiveConvertExtractor;
import java.util.ArrayList;
import java.util.List;

import gobblin.converter.Converter;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;
import gobblin.source.extractor.Extractor;

import lombok.Getter;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;


/**
 * Represents a gobblin Record in the Hive avro to orc conversion flow.
 * The {@link HiveConvertExtractor} extracts exactly one {@link QueryBasedHiveConversionEntity}.
 * This class is a container for all metadata about a {@link HiveTable} or a {@link HivePartition} needed to build
 * the hive conversion query.The gobblin task constructs can mutate this object as it get passed from
 * {@link Extractor} to {@link Converter}s.
 *
 * <ul>
 *  <li> The {@link HiveConvertExtractor} creates {@link QueryBasedHiveConversionEntity} using the {@link HiveRegistrationUnit}
 *  in the workunit
 *  <li> The {@link HiveAvroToOrcConverter} builds the {@link QueryBasedHiveConversionEntity#query} using
 *  {@link QueryBasedHiveConversionEntity#hiveUnitSchema}.
 *  <li> The {@link HiveQueryExecutionWriter} executes the hive query at {@link QueryBasedHiveConversionEntity#getConversionQuery()}
 * </ul>
 */
public class QueryBasedHiveConversionEntity {

  public QueryBasedHiveConversionEntity(HiveRegistrationUnit hiveUnit, Schema hiveUnitSchema) {
    this.hiveUnit = hiveUnit;
    this.hiveUnitSchema = hiveUnitSchema;
    this.queries = new ArrayList<>();
  }

  /**
   * A {@link StringBuilder} for the hive conversion query
   */
  private List<String> queries;

  /**
   * A {@link HiveTable} or a {@link HivePartition} to be converted
   */
  @Getter
  private HiveRegistrationUnit hiveUnit;

  /**
   * Avro {@link Schema} of the {@link HiveTable} or {@link HivePartition}
   */
  @Getter
  private Schema hiveUnitSchema;

  /**
   * Append <code>query</code> to the end of existing query
   * @return the instance with query appended
   */
  public QueryBasedHiveConversionEntity appendQuery(String query) {
    this.queries.add(query);
    return this;
  }

  /**
   * Get the final constructed hive query for conversion
   */
  public String getConversionQuery() {
    return StringUtils.join(this.queries.toString(), ";");
  }

  public List<String> getConversionQueries() {
    return this.queries;
  }
}
