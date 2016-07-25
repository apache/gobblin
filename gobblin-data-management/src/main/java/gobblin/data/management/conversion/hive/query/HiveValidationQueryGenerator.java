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

package gobblin.data.management.conversion.hive.query;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.copy.hive.HiveDataset;


/***
 * Generate Hive queries for validation
 *
 * @author Abhishek Tiwari
 */
@Slf4j
public class HiveValidationQueryGenerator {

  /***
   * Generate Hive queries for validating converted Hive table.
   * @param hiveDataset Source {@link HiveDataset}.
   * @param sourcePartition Source {@link Partition} if any.
   * @param conversionConfig {@link ConvertibleHiveDataset.ConversionConfig} for conversion.
   * @return Validation Hive queries.
   */
  public static List<String> generateValidationQueries(HiveDataset hiveDataset,
      Optional<Partition> sourcePartition,
      ConvertibleHiveDataset.ConversionConfig conversionConfig) {

    // Source and converted destination details
    String sourceDatabase = hiveDataset.getDbAndTable().getDb();
    String sourceTable = hiveDataset.getDbAndTable().getDb();
    String destinationDatabase = conversionConfig.getDestinationDbName();
    String destinationTable = conversionConfig.getDestinationTableName();

    // Build query.
    List<String> queries = Lists.newArrayList();

    if (sourcePartition.isPresent()) {
      queries.add(String
          .format("SELECT count(*) FROM `%s`.`%s` WHERE `%s`;", sourceDatabase, sourceTable, sourcePartition.get().getName()));
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s` WHERE `%s`;", destinationDatabase, destinationTable,
          sourcePartition.get().getName()));
    } else {
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s`; ", sourceDatabase, sourceTable));
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s`;", destinationDatabase, destinationTable));
    }

    return queries;
  }
}
