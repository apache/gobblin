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

package gobblin.compaction.hive;

import java.sql.SQLException;

import gobblin.util.HiveJdbcConnector;

/**
 * A class for managing Hive managed tables.
 */
public class HiveManagedTable extends HiveTable {
  public static class Builder extends HiveTable.Builder<Builder> {
    public HiveManagedTable build() {
      return new HiveManagedTable(this);
    }
  }

  private HiveManagedTable(HiveManagedTable.Builder builder) {
    super(builder);
  }

  public void createTable(HiveJdbcConnector conn, String jobId, String tableType) throws SQLException {

    String tableName = getNameWithJobId(jobId);
    String dropTableStmt = String.format(DROP_TABLE_STMT, tableName);

    StringBuilder sb = new StringBuilder().append("CREATE ");
    sb.append(tableType + " ");
    sb.append(tableName);
    sb.append('(');
    for (int i = 0; i < this.attributes.size(); i++) {
      sb.append(this.attributes.get(i).name() + " " + this.attributes.get(i).type());
      if (i != this.attributes.size() - 1) {
        sb.append(", ");
      }
    }
    sb.append(")");

    String createTableStmt = sb.toString();

    conn.executeStatements(dropTableStmt, createTableStmt);
  }

  @Override
  public void createTable(HiveJdbcConnector conn, String randomSuffix) throws SQLException {
    createTable(conn, randomSuffix, "TABLE");
  }

  public void createTemporaryTable(HiveJdbcConnector conn, String randomSuffix) throws SQLException {
    createTable(conn, randomSuffix, "TEMPORARY TABLE");
  }

  @Override
  public HiveTable addNewColumnsInSchema(HiveJdbcConnector conn, HiveTable table, String randomSuffix)
      throws SQLException {
    if (hasNoNewColumn(table)) {
      return this;
    }

    StringBuilder sb =
        new StringBuilder().append("ALTER TABLE " + this.getNameWithJobId(randomSuffix) + " ADD COLUMNS (");

    boolean addComma = false;
    for (HiveAttribute attribute : table.attributes) {
      if (!this.attributes.contains(attribute)) {
        if (addComma) {
          sb.append(", ");
        }
        sb.append(attribute.name() + " " + attribute.type());
        addComma = true;
        this.attributes.add(attribute);
      }
    }
    sb.append(')');

    String alterTableStmt = sb.toString();
    conn.executeStatements(alterTableStmt);
    return this;
  }
}
