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

import static gobblin.configuration.ConfigurationKeys.WRITER_PREFIX;
import gobblin.converter.jdbc.JdbcEntryData;

import java.sql.SQLException;

public interface JdbcBufferedInserter {

  public static final String WRITER_JDBC_INSERT_BATCH_SIZE = WRITER_PREFIX + ".jdbc.batch_size";
  public static final int DEFAULT_WRITER_JDBC_INSERT_BATCH_SIZE = 30;
  public static final String WRITER_JDBC_INSERT_BUFFER_SIZE = WRITER_PREFIX + ".jdbc.insert_buffer_size";
  public static final int DEFAULT_WRITER_JDBC_INSERT_BUFFER_SIZE = 1024 * 1024; //1 MBytes
  public static final int MAX_WRITER_JDBC_INSERT_BUFFER_SIZE = 10 * 1024 * 1024; //10 MBytes
  public static final String WRITER_JDBC_MAX_PARAM_SIZE = WRITER_PREFIX + ".jdbc.insert_max_param_size";
  public static final int DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE = 100000; //MySQL limit
  public static final String WRITER_JDBC_INSERT_RETRY_TIMEOUT = WRITER_PREFIX + ".jdbc.insert_retry_timeout";
  public static final int DEFAULT_WRITER_JDBC_INSERT_RETRY_TIMEOUT = 30; // in seconds
  public static final String WRITER_JDBC_INSERT_RETRY_MAX_ATTEMPT = WRITER_PREFIX + ".jdbc.insert_retry_max_attempt";
  public static final int DEFAULT_WRITER_JDBC_INSERT_RETRY_MAX_ATTEMPT = 5;

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
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException;

  /**
   * Flushes all the entries in buffer into JDBC RDBMS.
   * @param conn
   * @throws SQLException
   */
  public void flush() throws SQLException;
}
