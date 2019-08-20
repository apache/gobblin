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

package org.apache.gobblin.writer.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;
import org.apache.gobblin.converter.jdbc.JdbcEntryDatum;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PostgresBufferedInserter extends BaseJdbcBufferedInserter {

  public PostgresBufferedInserter(State state, Connection conn) {
    super(state, conn);
  }

  @Override
  protected String createPrepareStatementStr(int batchSize) {
    final String VALUE_FORMAT = "(%s)";
    StringBuilder sb = new StringBuilder(this.insertStmtPrefix);
    String values =
        String.format(VALUE_FORMAT, JOINER_ON_COMMA.useForNull("?").join(new String[this.columnNames.size()]));
    sb.append(values);
    for (int i = 1; i < batchSize; i++) {
      sb.append(',').append(values);
    }

    return sb.append(';').toString();
  }

  @Override
  protected boolean insertBatch(PreparedStatement pstmt)
      throws SQLException {
    int i = 0;
    pstmt.clearParameters();
    for (JdbcEntryData pendingEntry : PostgresBufferedInserter.this.pendingInserts) {
      for (JdbcEntryDatum datum : pendingEntry) {
        pstmt.setObject(++i, datum.getVal());
      }
    }
    log.debug("Executing SQL " + pstmt);
    return pstmt.execute();
  }
}
