package gobblin.writer.jdbc;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.mockito.ArgumentCaptor;
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
    final String table = "users";
    final int writeCount = 25;
    JdbcWriterCommands writerCommands = mock(JdbcWriterCommands.class);
    Connection conn = mock(Connection.class);

    try (JdbcWriter writer = new JdbcWriter(writerCommands, new State(), table, conn)) {
      for(int i = 0; i < writeCount; i++) {
        writer.write(null);
      }
      writer.commit();
      Assert.assertEquals(writer.recordsWritten(), (long) writeCount);
    }

    verify(writerCommands, times(writeCount)).insert(any(Connection.class), anyString(), any(JdbcEntryData.class));
    verify(conn, times(1)).commit();
    verify(conn, never()).rollback();
    verify(writerCommands, times(1)).flush(conn);
    verify(conn, times(1)).close();
  }

  @Test
  public void writeFailRollbackTest() throws SQLException, IOException {
    final String table = "users";
    JdbcWriterCommands writerCommands = mock(JdbcWriterCommands.class);
    Connection conn = mock(Connection.class);
    doThrow(RuntimeException.class).when(writerCommands).insert(any(Connection.class), anyString(), any(JdbcEntryData.class));
    JdbcWriter writer = new JdbcWriter(writerCommands, new State(), table, conn);

    try {
      writer.write(null);
      Assert.fail("Test case didn't throw Exception.");
    } catch (RuntimeException e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }
    writer.close();

    verify(writerCommands, times(1)).insert(any(Connection.class), anyString(), any(JdbcEntryData.class));
    verify(conn, times(1)).rollback();
    verify(conn, never()).commit();
    verify(conn, times(1)).close();
    Assert.assertEquals(writer.recordsWritten(), 0L);
  }
}
