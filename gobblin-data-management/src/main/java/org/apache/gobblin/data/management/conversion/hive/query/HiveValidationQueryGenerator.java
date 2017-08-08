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

package org.apache.gobblin.data.management.conversion.hive.query;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;


/***
 * Generate Hive queries for validation
 *
 * @author Abhishek Tiwari
 */
public class HiveValidationQueryGenerator {

  /***
   * Generate Hive queries for validating converted Hive table.
   * @param hiveDataset Source {@link HiveDataset}.
   * @param sourcePartition Source {@link Partition} if any.
   * @param conversionConfig {@link ConvertibleHiveDataset.ConversionConfig} for conversion.
   * @return Validation Hive queries.
   */
  public static List<String> generateCountValidationQueries(HiveDataset hiveDataset,
      Optional<Partition> sourcePartition, ConvertibleHiveDataset.ConversionConfig conversionConfig) {

    // Source and converted destination details
    String sourceDatabase = hiveDataset.getDbAndTable().getDb();
    String sourceTable = hiveDataset.getDbAndTable().getTable();
    String destinationDatabase = conversionConfig.getDestinationDbName();
    String destinationTable = conversionConfig.getDestinationTableName();

    // Build query.
    List<String> queries = Lists.newArrayList();

    if (sourcePartition.isPresent()) {
      StringBuilder partitionClause = new StringBuilder();

      boolean isFirst = true;
      String partitionInfo = sourcePartition.get().getName();
      List<String> pInfo = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionInfo);
      for (String aPInfo : pInfo) {
        List<String> pInfoParts = Splitter.on("=").omitEmptyStrings().trimResults().splitToList(aPInfo);
        if (pInfoParts.size() != 2) {
          throw new IllegalArgumentException(String
              .format("Partition details should be of the format " + "partitionName=partitionValue. Recieved: %s",
                  aPInfo));
        }
        if (isFirst) {
          isFirst = false;
        } else {
          partitionClause.append(" and ");
        }
        partitionClause.append("`").append(pInfoParts.get(0)).append("`='").append(pInfoParts.get(1)).append("'");
      }

      queries.add(String
          .format("SELECT count(*) FROM `%s`.`%s` WHERE %s ", sourceDatabase, sourceTable, partitionClause));
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s` WHERE %s ", destinationDatabase, destinationTable,
          partitionClause));
    } else {
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s` ", sourceDatabase, sourceTable));
      queries.add(String.format("SELECT count(*) FROM `%s`.`%s` ", destinationDatabase, destinationTable));
    }

    return queries;
  }

  /***
   * Generates Hive SQL that can be used to validate the quality between two {@link Table}s or optionally
   * {@link Partition}. The query returned is a basic join query that returns the number of records matched
   * between the two {@link Table}s.
   * The responsibility of actually comparing this value with the expected module should be implemented by
   * the user.
   *
   * @param sourceTable Source Hive {@link Table} name.
   * @param sourceDb Source Hive database name.
   * @param targetTable Target Hive {@link Table} name.
   * @param optionalPartition Optional {@link Partition} to limit the comparison.
   * @return Query to find number of rows common between two tables.
   */
  public static String generateDataValidationQuery(String sourceTable, String sourceDb, Table targetTable,
      Optional<Partition> optionalPartition, boolean isNestedORC) {

    StringBuilder sb = new StringBuilder();

    // Query head
    sb.append("SELECT count(*) FROM `")
        .append(sourceDb).append("`.`").append(sourceTable).append("` s JOIN `")
        .append(targetTable.getDbName()).append("`.`").append(targetTable.getTableName()).append("` t ON \n");

    // Columns equality
    boolean isFirst = true;
    List<FieldSchema> fieldList = targetTable.getSd().getCols();
    for (FieldSchema field : fieldList) {

      // Do not add maps in the join clause. Hive does not support map joins LIHADOOP-21956
      if (StringUtils.startsWithIgnoreCase(field.getType(), "map")) {
        continue;
      }

      if (StringUtils.containsIgnoreCase(field.getType(), ":map")) {
        continue;
      }

      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(" AND \n");
      }

      if (isNestedORC) {
        sb.append("\ts.`").append(field.getName()).append("`<=>");
      } else {
        // The source column lineage information is available in field's comment. Remove the description prefix "from flatten_source"
        String colName = field.getComment().replaceAll("from flatten_source ", "").trim();
        sb.append("\ts.`").append(colName.replaceAll("\\.", "`.`")).append("`<=>");
      }
      sb.append("t.`").append(field.getName()).append("` ");
    }
    sb.append("\n");

    // Partition projection
    if (optionalPartition.isPresent()) {
      Partition partition = optionalPartition.get();
      String partitionsInfoString = partition.getName();
      List<String> pInfo = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsInfoString);
      for (int i = 0; i < pInfo.size(); i++) {
        List<String> partitionInfoParts = Splitter.on("=").omitEmptyStrings().trimResults().splitToList(pInfo.get(i));

        if (partitionInfoParts.size() != 2) {
          throw new IllegalArgumentException(
              String.format("Partition details should be of the format partitionName=partitionValue. Recieved: %s", pInfo.get(i)));
        }
        if (i==0) {
          // add where clause
          sb.append(" WHERE \n");
        } else {
          sb.append(" AND ");
        }
        // add project for source and destination partition
        sb.append(String.format("s.`%s`='%s' ", partitionInfoParts.get(0), partitionInfoParts.get(1)));
        sb.append(" AND ");
        sb.append(String.format("t.`%s`='%s' ", partitionInfoParts.get(0), partitionInfoParts.get(1)));
      }
    }

    return sb.toString();
  }
}
