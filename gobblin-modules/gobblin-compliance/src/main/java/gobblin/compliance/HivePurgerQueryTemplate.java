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
package gobblin.compliance;

import java.util.ArrayList;
import java.util.List;


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
  private static String getUseDbQuery(String dbName) {
    return "USE " + dbName;
  }

  private static String getAutoConvertJoinProperty() {
    return "SET hive.auto.convert.join=false";
  }

  /**
   * Will allow insert query to specify only partition coloumn names instead of partition spec.
   */
  private static String getDynamicPartitionNonStrictModeProperty() {
    return "SET hive.exec.dynamic.partition.mode=nonstrict";
  }

  /**
   * If staging table doesn't exist, it will create a staging table.
   */
  private static String getCreateStagingTableQuery(ComplianceRecord record) {
    return "CREATE EXTERNAL TABLE IF NOT EXISTS " +
        record.getCompleteStagingTableName() + " LIKE " +
        record.getDbName() + "." + record.getTableName() +
        " LOCATION " + ComplianceRecord.getQuotedString(record.getStagingTableLocation());
  }

  /**
   * This query will create a partition in staging table and insert the records whose compliance id is not
   * contained in the compliance id table.
   */
  private static String getInsertQuery(ComplianceRecord record) {
    return "INSERT OVERWRITE" +
        " TABLE " + record.getCompleteStagingTableName() +
        " PARTITION (" + record.getCommaSeparatedPartitionColumnNames() + ")" +
        " SELECT /*+MAPJOIN(b) */ a.* FROM " +
        record.getDbName() + "." + record.getTableName() +
        " a LEFT JOIN " + record.getComplianceIdTable() + " b" +
        " ON a." + record.getDatasetComplianceId() + "=b." + record.getComplianceIdentifier() +
        " WHERE b." + record.getComplianceIdentifier() + " IS NULL AND " + record.getWhereClauseForPartition("a.");
  }

  /**
   * This query will alter the location of original table partition to point to the final partition location.
   */
  private static String getAlterTableQuery(ComplianceRecord record) {
    return "ALTER TABLE " +
        record.getTableName() +
        " PARTITION (" + record.getPartitionSpec() + ")" +
        " SET LOCATION " + ComplianceRecord.getQuotedString(record.getFinalPartitionLocation());
  }

  /**
   * Will return all the queries needed to populate the staging table partition.
   * This won't include alter table partition location query.
   */
  public static List<String> getPurgeQueries(ComplianceRecord record) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(record.getStagingDb()));
    queries.add(getDynamicPartitionNonStrictModeProperty());
    queries.add(getAutoConvertJoinProperty());
    queries.add(getCreateStagingTableQuery(record));
    queries.add(getInsertQuery(record));
    return queries;
  }

  /**
   * Will return all the queries needed to alter the location of the table partition.
   * Alter table partition query doesn't work with syntax dbName.tableName
   */
  public static List<String> getAlterTableQueries(ComplianceRecord record) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(record.getDbName()));
    queries.add(getAlterTableQuery(record));
    return queries;
  }
}
