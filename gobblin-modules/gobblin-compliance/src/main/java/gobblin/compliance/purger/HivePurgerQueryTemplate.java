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
package gobblin.compliance.purger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Optional;

import gobblin.compliance.utils.PartitionUtils;


/**
 * This class creates all queries required by {@link HivePurgerConverter}
 *
 * @author adsharma
 */
public class HivePurgerQueryTemplate {

  /**
   * Use the staging db for creating staging tables.
   * Alter query doesn't work with dbName.tableName.
   */
  public static String getUseDbQuery(String dbName) {
    return "USE " + dbName;
  }

  public static String getAutoConvertJoinProperty() {
    return "SET hive.auto.convert.join=false";
  }

  /**
   * Will allow insert query to specify only partition coloumn names instead of partition spec.
   */
  public static String getDynamicPartitionNonStrictModeProperty() {
    return "SET hive.exec.dynamic.partition.mode=nonstrict";
  }

  /**
   * To get around the OOM, we need to set hive.optimize.sort.dynamic.partition to true so that the data is sorted by
   * partition key and at one time only one partition needs to be accessed.
   */
  public static String getOptimizeSortDynamicPartition() {
    return "SET hive.optimize.sort.dynamic.partition=true";
  }

  /**
   * If staging table doesn't exist, it will create a staging table.
   */
  public static String getCreateTableQuery(String completeNewTableName, String likeTableDbName, String likeTableName,
      String location) {
    return getCreateTableQuery(completeNewTableName, likeTableDbName, likeTableName) + " LOCATION " + PartitionUtils
        .getQuotedString(location);
  }

  public static String getCreateTableQuery(String completeNewTableName, String likeTableDbName, String likeTableName) {
    return "CREATE EXTERNAL TABLE IF NOT EXISTS " + completeNewTableName + " LIKE " + likeTableDbName + "."
        + likeTableName;
  }

  /**
   * This query will create a partition in staging table and insert the datasets whose compliance id is not
   * contained in the compliance id table.
   */
  public static String getInsertQuery(PurgeableHivePartitionDataset dataset) {
    return "INSERT OVERWRITE" + " TABLE " + dataset.getCompleteStagingTableName() + " PARTITION (" + PartitionUtils
        .getPartitionSpecString(dataset.getSpec()) + ")" + " SELECT /*+MAPJOIN(b) */ " + getCommaSeparatedColumnNames(
        dataset.getCols(), "a.") + " FROM " + dataset.getDbName() + "." + dataset.getTableName() + " a LEFT JOIN "
        + dataset.getComplianceIdTable() + " b" + " ON a." + dataset.getComplianceField() + "=b." + dataset
        .getComplianceId() + " WHERE b." + dataset.getComplianceId() + " IS NULL AND " + getWhereClauseForPartition(
        dataset.getSpec(), "a.");
  }

  public static String getAddPartitionQuery(String tableName, String partitionSpec, Optional<String> fileFormat, Optional<String> location) {
    String query = "ALTER TABLE " + tableName + " ADD IF NOT EXISTS" + " PARTITION (" + partitionSpec + ")";
    if (fileFormat.isPresent()) {
      query = query + " FILEFORMAT " + fileFormat.get();
    }
    if (location.isPresent()) {
      query = query + " LOCATION " + PartitionUtils.getQuotedString(location.get());
    }
    return query;
  }

  public static String getAlterTableLocationQuery(String tableName, String partitionSpec, String location) {
    return "ALTER TABLE " + tableName + " PARTITION (" + partitionSpec + ")" + " SET LOCATION " + PartitionUtils
        .getQuotedString(location);
  }

  public static String getDropTableQuery(String dbName, String tableName) {
    return "DROP TABLE IF EXISTS " + dbName + "." + tableName;
  }

  public static String getDropPartitionQuery(String tableName, String partitionSpec) {
    return "ALTER TABLE " + tableName + " DROP IF EXISTS" + " PARTITION (" + partitionSpec + ")";
  }

  public static String getUpdatePartitionMetadataQuery(String dbName, String tableName, String partitionSpec) {
    return "ANALYZE TABLE " + dbName + "." + tableName + " PARTITION (" + partitionSpec + ") COMPUTE STATISTICS";
  }

  /**
   * Will return all the queries needed to populate the staging table partition.
   * This won't include alter table partition location query.
   */
  public static List<String> getPurgeQueries(PurgeableHivePartitionDataset dataset) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(dataset.getStagingDb()));
    queries.add(getInsertQuery(dataset));
    return queries;
  }

  public static List<String> getCreateStagingTableQuery(PurgeableHivePartitionDataset dataset) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(dataset.getStagingDb()));
    queries.add(getAutoConvertJoinProperty());
    queries.add(getCreateTableQuery(dataset.getCompleteStagingTableName(), dataset.getDbName(), dataset.getTableName(),
        dataset.getStagingTableLocation()));

    Optional<String> fileFormat = Optional.absent();
    if (dataset.getSpecifyPartitionFormat()) {
      fileFormat = dataset.getFileFormat();
    }
    queries.add(getAddPartitionQuery(dataset.getCompleteStagingTableName(),
        PartitionUtils.getPartitionSpecString(dataset.getSpec()), fileFormat,
        Optional.fromNullable(dataset.getStagingPartitionLocation())));
    return queries;
  }

  /**
   * Will return all the queries needed to have a backup table partition pointing to the original partition data location
   */
  public static List<String> getBackupQueries(PurgeableHivePartitionDataset dataset) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(dataset.getDbName()));
    queries.add(getCreateTableQuery(dataset.getCompleteBackupTableName(), dataset.getDbName(), dataset.getTableName(),
        dataset.getBackupTableLocation()));
    Optional<String> fileFormat = Optional.absent();
    if (dataset.getSpecifyPartitionFormat()) {
      fileFormat = dataset.getFileFormat();
    }
    queries.add(
        getAddPartitionQuery(dataset.getBackupTableName(), PartitionUtils.getPartitionSpecString(dataset.getSpec()),
            fileFormat, Optional.fromNullable(dataset.getOriginalPartitionLocation())));
    return queries;
  }

  /**
   * Will return all the queries needed to alter the location of the table partition.
   * Alter table partition query doesn't work with syntax dbName.tableName
   */
  public static List<String> getAlterOriginalPartitionLocationQueries(PurgeableHivePartitionDataset dataset) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(dataset.getDbName()));
    String partitionSpecString = PartitionUtils.getPartitionSpecString(dataset.getSpec());
    queries.add(
        getAlterTableLocationQuery(dataset.getTableName(), partitionSpecString, dataset.getStagingPartitionLocation()));
    queries.add(getUpdatePartitionMetadataQuery(dataset.getDbName(), dataset.getTableName(), partitionSpecString));
    return queries;
  }

  public static List<String> getDropStagingTableQuery(PurgeableHivePartitionDataset dataset) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(dataset.getStagingDb()));
    queries.add(
        getDropPartitionQuery(dataset.getStagingTableName(), PartitionUtils.getPartitionSpecString(dataset.getSpec())));
    return queries;
  }

  /**
   * This method builds the where clause for the insertion query.
   * If prefix is a, then it builds a.datepartition='2016-01-01-00' AND a.size='12345' from [datepartition : '2016-01-01-00', size : '12345']
   */
  public static String getWhereClauseForPartition(Map<String, String> spec, String prefix) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : spec.entrySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(" AND ");
      }
      sb.append(prefix + entry.getKey());
      sb.append("=");
      sb.append(PartitionUtils.getQuotedString(entry.getValue()));
    }
    return sb.toString();
  }

  public static String getCommaSeparatedColumnNames(List<FieldSchema> cols, String prefix) {
    StringBuilder sb = new StringBuilder();
    for (FieldSchema fs : cols) {
      if (!sb.toString().isEmpty()) {
        sb.append(", ");
      }
      sb.append(prefix);
      sb.append(fs.getName());
    }
    return sb.toString();
  }
}
