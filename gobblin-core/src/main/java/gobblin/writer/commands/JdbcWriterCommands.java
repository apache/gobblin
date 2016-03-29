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
package gobblin.writer.commands;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map;

public interface JdbcWriterCommands extends JdbcBufferedInserter {

  /**
   * Creates table structure based on the table in parameter. Note that this won't guarantee copy exactly the same as
   * original table such as restraints, foreign keys, sequences, indices, etc
   * @param conn
   * @param fromStructure
   * @param targetTableName
   * @throws SQLException
   */
  public void createTableStructure(Connection conn, String fromStructure, String targetTableName) throws SQLException;

  /**
   * Check if table is empty.
   * @param conn
   * @param table
   * @return
   * @throws SQLException
   */
  public boolean isEmpty(Connection conn, String table) throws SQLException;

  /**
   * Truncates table. Most RDBMS cannot be rollback from this operation.
   * @param conn
   * @param table table name to be truncated.
   * @throws SQLException
   */
  public void truncate(Connection conn, String table) throws SQLException;

  /**
   * Deletes all contents from the table. This method can be rollback if not committed.
   * @param conn
   * @param table
   * @throws SQLException
   */
  public void deleteAll(Connection conn, String table) throws SQLException;

  /**
   * Drops the table.
   * @param conn
   * @param table
   * @throws SQLException
   */
  public void drop(Connection conn, String table) throws SQLException;

  /**
   * Retrieves date related column such as Date, Time, DateTime, Timestamp etc.
   * @param conn
   * @param table
   * @return Map of column name and JDBCType that is date related.
   * @throws SQLException
   */
  public Map<String, JDBCType> retrieveDateColumns(Connection conn, String table) throws SQLException;

  /**
   * Copy all the contents from one table to another. Both table should be in same structure.
   * @param conn
   * @param databaseName
   * @param from
   * @param to
   * @throws SQLException
   */
  public void copyTable(Connection conn, String databaseName, String from, String to) throws SQLException;
}