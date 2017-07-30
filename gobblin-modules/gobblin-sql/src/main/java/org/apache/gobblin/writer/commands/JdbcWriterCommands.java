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
package gobblin.writer.commands;

import gobblin.converter.jdbc.JdbcType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * JdbcWriterCommands is interface that its implementation will
 * directly talk with underlying RDBMS via JDBC. Different RDBMS may use different SQL syntax,
 * and having this interface decouples JdbcWriter with any syntax difference each RDBMS might have.
 */
public interface JdbcWriterCommands extends JdbcBufferedInserter {

  /**
   * Sets writer specific connection parameters, e.g transaction handling
   *
   * @param properties State properties
   * @param conn Connection object
   */
  public void setConnectionParameters(Properties properties, Connection conn) throws SQLException;

  /**
   * Creates table structure based on the table in parameter. Note that this won't guarantee copy exactly the same as
   * original table such as restraints, foreign keys, sequences, indices, etc
   * @param fromStructure
   * @param targetTableName
   * @throws SQLException
   */
  public void createTableStructure(String databaseName, String fromStructure, String targetTableName) throws SQLException;

  /**
   * Check if table is empty.
   * @param table
   * @return
   * @throws SQLException
   */
  public boolean isEmpty(String database, String table) throws SQLException;

  /**
   * Truncates table. Most RDBMS cannot be rollback from this operation.
   * @param table table name to be truncated.
   * @throws SQLException
   */
  public void truncate(String database, String table) throws SQLException;

  /**
   * Deletes all contents from the table. This method can be rollback if not committed.
   * @param table
   * @throws SQLException
   */
  public void deleteAll(String database, String table) throws SQLException;

  /**
   * Drops the table.
   * @param table
   * @throws SQLException
   */
  public void drop(String database, String table) throws SQLException;

  /**
   * Retrieves date related column such as Date, Time, DateTime, Timestamp etc.
   * @param database
   * @param table
   * @return Map of column name and JdbcType that is date related.
   * @throws SQLException
   */
  public Map<String, JdbcType> retrieveDateColumns(String database, String table) throws SQLException;

  /**
   * Copy all the contents from one table to another. Both table should be in same structure.
   * @param databaseName
   * @param from
   * @param to
   * @throws SQLException
   */
  public void copyTable(String databaseName, String from, String to) throws SQLException;
}