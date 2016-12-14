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
package gobblin.compliance;

import java.util.ArrayList;
import java.util.List;


/**
 * This class creates all queries required by {@link HivePurgerConverter}
 *
 * @author adsharma
 */
public class HivePurgerQueryTemplate {

  public static String getUseDbQuery(HivePurgerPartitionRecord record) {
    return "USE " + record.getHiveTablePartition().getTable().getDbName();
  }

  public static String getAutoConvertJoinProperty() {
    return "SET hive.auto.convert.join=false";
  }

  public static String getDynamicPartitionNonStrictModeProperty() {
    return "SET hive.exec.dynamic.partition.mode=nonstrict";
  }

  public static String getCreateStagingTableQuery(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    sb.append(DatasetUtils.getCompleteStagingTableName(record));
    sb.append(" LIKE ");
    sb.append(record.getHiveTablePartition().getTable().getDbName());
    sb.append(".");
    sb.append(record.getHiveTablePartition().getTable().getTableName());
    sb.append(" LOCATION '");
    sb.append(DatasetUtils.getStagingTableLocation(record));
    sb.append("'");
    return sb.toString();
  }

  public static String getInsertQuery(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT OVERWRITE TABLE ");
    sb.append(DatasetUtils.getCompleteStagingTableName(record));
    sb.append(" PARTITION (");
    sb.append(DatasetUtils.getCommaSeparatedPartitionColumnNames(record));
    sb.append(") SELECT /*+MAPJOIN(b) */ a.*");
    sb.append(" from ");
    sb.append(record.getHiveTablePartition().getTable().getDbName());
    sb.append(".");
    sb.append(record.getHiveTablePartition().getTable().getTableName());
    sb.append(" a LEFT JOIN ");
    sb.append(record.getComplianceIdTable());
    sb.append(" b ON a.");
    sb.append(record.getDatasetDescriptor().getComplianceId());
    sb.append("=b.");
    sb.append(record.getComplianceIdentifier());
    sb.append(" WHERE b.");
    sb.append(record.getComplianceIdentifier());
    sb.append(" IS NULL AND ");
    sb.append(DatasetUtils.getWhereClauseForPartition("a.", record));
    return sb.toString();
  }

  public static String getAlterTableQuery(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ");
    sb.append(DatasetUtils.getCompleteOriginalTableName(record));
    sb.append(" PARTITION (");
    sb.append(DatasetUtils.getPartitionSpec(record));
    sb.append(") SET LOCATION '");
    sb.append(DatasetUtils.getFinalPartitionLocation(record));
    sb.append("'");
    return sb.toString();
  }

  public static List<String> getPurgeQueries(HivePurgerPartitionRecord record) {
    List<String> queries = new ArrayList<>();
    queries.add(getUseDbQuery(record));
    queries.add(getDynamicPartitionNonStrictModeProperty());
    queries.add(getAutoConvertJoinProperty());
    queries.add(getCreateStagingTableQuery(record));
    queries.add(getInsertQuery(record));
    return queries;
  }
}
