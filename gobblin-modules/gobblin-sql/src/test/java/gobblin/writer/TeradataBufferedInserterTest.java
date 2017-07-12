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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.mockrunner.mock.jdbc.MockParameterMetaData;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.writer.commands.JdbcBufferedInserter;
import gobblin.writer.commands.TeradataBufferedInserter;


@Test(groups = { "gobblin.writer" }, singleThreaded = true)
public class TeradataBufferedInserterTest extends JdbcBufferedInserterTestBase {

  public void testTeradataBufferedInsert() throws SQLException {
    final int colNums = 20;
    final int batchSize = 10;
    final int entryCount = 107;
    final int colSize = 7;

    State state = new State();
    state.setProp(WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));

    JdbcBufferedInserter inserter = getJdbcBufferedInserter(state, conn);
    MockParameterMetaData mockMetadata = new MockParameterMetaData();
    mockMetadata.setParameterCount(2);
    mockMetadata.setParameterType(0, 12);
    mockMetadata.setParameterType(1, -5);

    PreparedStatement pstmt = Mockito.mock(PreparedStatement.class);
    when(pstmt.getParameterMetaData()).thenReturn(mockMetadata);
    when(pstmt.executeBatch()).thenReturn(new int[] { 1, 1, 1 });
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for (JdbcEntryData entry : jdbcEntries) {
      inserter.insert(db, table, entry);
    }
    inserter.flush();

    verify(conn, times(2)).prepareStatement(anyString());
    verify(pstmt, times(107)).addBatch();
    verify(pstmt, times((int) Math.ceil((double) entryCount / batchSize))).executeBatch();
    verify(pstmt, times(entryCount)).clearParameters();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
    reset(pstmt);
  }

  @Override
  protected JdbcBufferedInserter getJdbcBufferedInserter(State state, Connection conn) {
    return new TeradataBufferedInserter(state, conn);
  }

}
