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

import gobblin.converter.jdbc.JdbcEntryData;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcBufferedInserter {

  /**
   * Inserts entry. Depends on the current batch size, buffer size, param size, it can either put into buffer
   * or it will actually call underlying JDBC RDBMS to be inserted.
   *
   * The number of input columns is expected to be equal or smaller than the number of columns in Jdbc.
   * This is to prevent unintended outcome from schema evolution such as additional column.
   * As underlying Jdbc RDBMS can declare constraints on its schema, writer will allow if number of columns in Jdbc is greater than number of input columns.
   *    number of input columns <= number of columns in Jdbc
   *
   * @param conn
   * @param databaseName
   * @param table
   * @param jdbcEntryData
   * @throws SQLException
   */
  public void insert(Connection conn, String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException;

  /**
   * Flushes all the entries in buffer into JDBC RDBMS.
   * @param conn
   * @throws SQLException
   */
  public void flush(Connection conn) throws SQLException;
}
