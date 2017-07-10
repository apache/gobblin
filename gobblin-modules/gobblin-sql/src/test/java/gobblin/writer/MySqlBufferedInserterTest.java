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

package gobblin.writer;

import static gobblin.writer.commands.JdbcBufferedInserter.WRITER_JDBC_INSERT_BATCH_SIZE;
import static gobblin.writer.commands.JdbcBufferedInserter.WRITER_JDBC_MAX_PARAM_SIZE;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.writer.commands.JdbcBufferedInserter;
import gobblin.writer.commands.MySqlBufferedInserter;

@Test(groups = {"gobblin.writer"}, singleThreaded=true)
public class MySqlBufferedInserterTest extends JdbcBufferedInserterTestBase {

  public void testMySqlBufferedInsert() throws SQLException {
    final int colNums = 20;
    final int batchSize = 10;
    final int entryCount = 107;
    final int colSize = 7;

    State state = new State();
    state.setProp(WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));

    JdbcBufferedInserter inserter = getJdbcBufferedInserter(state, conn);

    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for(JdbcEntryData entry : jdbcEntries) {
      inserter.insert(db, table, entry);
    }
    inserter.flush();

    verify(conn, times(2)).prepareStatement(anyString());
    verify(pstmt, times(11)).clearParameters();
    verify(pstmt, times(11)).execute();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
    reset(pstmt);
  }

  public void testMySqlBufferedInsertParamLimit() throws SQLException {
    final int colNums = 50;
    final int batchSize = 10;
    final int entryCount = 107;
    final int colSize = 3;
    final int maxParamSize = 500;

    State state = new State();
    state.setProp(WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));
    state.setProp(WRITER_JDBC_MAX_PARAM_SIZE, maxParamSize);

    MySqlBufferedInserter inserter = new MySqlBufferedInserter(state, conn);

    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for(JdbcEntryData entry : jdbcEntries) {
      inserter.insert(db, table, entry);
    }
    inserter.flush();

    int expectedBatchSize = maxParamSize / colNums;
    int expectedExecuteCount = entryCount / expectedBatchSize + 1;
    verify(conn, times(2)).prepareStatement(anyString());
    verify(pstmt, times(expectedExecuteCount)).clearParameters();
    verify(pstmt, times(expectedExecuteCount)).execute();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
    reset(pstmt);
  }

  @Override
  protected JdbcBufferedInserter getJdbcBufferedInserter(State state, Connection conn) {
    return new MySqlBufferedInserter(state, conn);
  }
}
