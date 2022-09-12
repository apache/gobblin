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

package org.apache.gobblin.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcType;
import org.apache.gobblin.writer.commands.PostgresWriterCommands;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.sun.rowset.JdbcRowSetImpl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Test(groups = {"gobblin.writer"})
public class PostgresWriterCommandsTest {
  @Test
  public void testPostgresDateTypeRetrieval()
      throws SQLException {
    Connection conn = mock(Connection.class);

    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(any(String.class), any(Integer.class), any(Integer.class))).thenReturn(pstmt);

    ResultSet rs = createMockResultSet();
    when(pstmt.executeQuery()).thenReturn(rs);

    PostgresWriterCommands writerCommands = new PostgresWriterCommands(new State(), conn, false);
    Map<String, JdbcType> actual = writerCommands.retrieveDateColumns("db", "users");

    ImmutableMap.Builder<String, JdbcType> builder = ImmutableMap.builder();
    builder.put("date_of_birth", JdbcType.DATE);
    builder.put("last_modified", JdbcType.TIME);
    builder.put("created", JdbcType.TIMESTAMP);

    Map<String, JdbcType> expected = builder.build();

    Assert.assertEquals(expected, actual);
  }

  private ResultSet createMockResultSet() {
    final List<Map<String, String>> expected = new ArrayList<>();
    Map<String, String> entry = new HashMap<>();
    entry.put("column_name", "name");
    entry.put("data_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "favorite_number");
    entry.put("data_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "favorite_color");
    entry.put("data_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "date_of_birth");
    entry.put("data_type", "date");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "last_modified");
    entry.put("data_type", "time without time zone");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "created");
    entry.put("data_type", "timestamp with time zone");
    expected.add(entry);

    return new JdbcRowSetImpl() {
      private Iterator<Map<String, String>> it = expected.iterator();
      private Map<String, String> curr = null;

      @Override
      public boolean first() {
        it = expected.iterator();
        return next();
      }

      @Override
      public boolean next() {
        if (it.hasNext()) {
          curr = it.next();
          return true;
        }
        return false;
      }

      @Override
      public String getString(String columnLabel)
          throws SQLException {
        if (curr == null) {
          throw new SQLException("NPE on current cursor.");
        }
        String val = curr.get(columnLabel);
        if (val == null) {
          throw new SQLException(columnLabel + " does not exist.");
        }
        return val;
      }
    };
  }
}
