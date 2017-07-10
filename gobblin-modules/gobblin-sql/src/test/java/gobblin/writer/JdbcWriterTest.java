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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.writer.JdbcWriter;
import gobblin.writer.commands.JdbcWriterCommands;

@Test(groups = {"gobblin.writer"})
public class JdbcWriterTest {

  @Test
  public void writeAndCommitTest() throws SQLException, IOException {
    final String database = "db";
    final String table = "users";
    final int writeCount = 25;
    JdbcWriterCommands writerCommands = mock(JdbcWriterCommands.class);
    Connection conn = mock(Connection.class);

    try (JdbcWriter writer = new JdbcWriter(writerCommands, new State(), database, table, conn)) {
      for(int i = 0; i < writeCount; i++) {
        writer.write(null);
      }
      writer.commit();
      Assert.assertEquals(writer.recordsWritten(), writeCount);
    }

    verify(writerCommands, times(writeCount)).insert(anyString(), anyString(), any(JdbcEntryData.class));
    verify(conn, times(1)).commit();
    verify(conn, never()).rollback();
    verify(writerCommands, times(1)).flush();
    verify(conn, times(1)).close();
  }

  @Test
  public void writeFailRollbackTest() throws SQLException, IOException {
    final String database = "db";
    final String table = "users";
    JdbcWriterCommands writerCommands = mock(JdbcWriterCommands.class);
    Connection conn = mock(Connection.class);
    doThrow(RuntimeException.class).when(writerCommands).insert(anyString(), anyString(), any(JdbcEntryData.class));
    JdbcWriter writer = new JdbcWriter(writerCommands, new State(), database, table, conn);

    try {
      writer.write(null);
      Assert.fail("Test case didn't throw Exception.");
    } catch (RuntimeException e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }
    writer.close();

    verify(writerCommands, times(1)).insert(anyString(), anyString(), any(JdbcEntryData.class));
    verify(conn, times(1)).rollback();
    verify(conn, never()).commit();
    verify(conn, times(1)).close();
    Assert.assertEquals(writer.recordsWritten(), 0L);
  }
}
