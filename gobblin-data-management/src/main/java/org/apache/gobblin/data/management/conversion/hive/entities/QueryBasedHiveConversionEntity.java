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
package gobblin.data.management.conversion.hive.entities;

import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.converter.Converter;
import gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.extractor.HiveConvertExtractor;
import gobblin.data.management.conversion.hive.writer.HiveQueryExecutionWriter;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;
import gobblin.source.extractor.Extractor;


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
 *  <li> The {@link AbstractAvroToOrcConverter} builds the {@link QueryBasedHiveConversionEntity#query} using
 *  {@link QueryBasedHiveConversionEntity#hiveUnitSchema}.
 *  <li> The {@link HiveQueryExecutionWriter} executes the hive query at {@link QueryBasedHiveConversionEntity#getConversionQuery()}
 * </ul>
 */
@ToString
@EqualsAndHashCode
@Getter
public class QueryBasedHiveConversionEntity {

  private final ConvertibleHiveDataset convertibleHiveDataset;
  private final SchemaAwareHiveTable hiveTable;
  private final Optional<SchemaAwareHivePartition> hivePartition;

  /**
   * A {@link StringBuilder} for the hive conversion query
   */
  private final List<String> queries;

  public QueryBasedHiveConversionEntity(ConvertibleHiveDataset convertibleHiveDataset, SchemaAwareHiveTable hiveTable) {
    this(convertibleHiveDataset, hiveTable, Optional.<SchemaAwareHivePartition> absent());
  }

  public QueryBasedHiveConversionEntity(ConvertibleHiveDataset convertibleHiveDataset, SchemaAwareHiveTable hiveTable,
      Optional<SchemaAwareHivePartition> hivePartition) {
    this.convertibleHiveDataset = convertibleHiveDataset;
    this.hiveTable = hiveTable;
    this.hivePartition = hivePartition;
    this.queries = Lists.newArrayList();
  }
}
