package gobblin.writer.jdbc;

import static org.mockito.Mockito.*;
import gobblin.configuration.State;
import gobblin.writer.commands.MySqlWriterCommands;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import junit.framework.Assert;

import com.google.common.collect.ImmutableMap;
import com.sun.rowset.JdbcRowSetImpl;

@Test(groups = {"gobblin.writer"})
public class JdbcWriterCommandsTest {
  @Test
  public void testMySqlDateTypeRetrieval() throws SQLException {
    Connection conn = mock(Connection.class);

    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(any(String.class))).thenReturn(pstmt);

    ResultSet rs = createMockResultSet();
    when(pstmt.executeQuery()).thenReturn(rs);

    MySqlWriterCommands writerCommands = new MySqlWriterCommands(new State());
    Map<String, JDBCType> actual = writerCommands.retrieveDateColumns(conn, "users");

    ImmutableMap.Builder<String, JDBCType> builder = ImmutableMap.builder();
    builder.put("date_of_birth",JDBCType.DATE);
    builder.put("last_modified", JDBCType.TIME);
    builder.put("created", JDBCType.TIMESTAMP);

    Map<String, JDBCType> expected = builder.build();

    Assert.assertEquals(expected, actual);
  }



  private ResultSet createMockResultSet() {
    final List<Map<String, String>> expected = new ArrayList<>();
    Map<String, String> entry = new HashMap<>();
    entry.put("column_name", "name");
    entry.put("column_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "favorite_number");
    entry.put("column_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "favorite_color");
    entry.put("column_type", "varchar");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "date_of_birth");
    entry.put("column_type", "date");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "last_modified");
    entry.put("column_type", "time");
    expected.add(entry);

    entry = new HashMap<>();
    entry.put("column_name", "created");
    entry.put("column_type", "timestamp");
    expected.add(entry);

    return new JdbcRowSetImpl(){
      private Iterator<Map<String, String>> it = expected.iterator();
      private Map<String, String> curr = null;

      @Override
      public boolean first() {
        it = expected.iterator();
        return next();
      }

      @Override
      public boolean next() {
        if(it.hasNext()) {
          curr = it.next();
          return true;
        }
        return false;
      }

      @Override
      public String getString(String columnLabel) throws SQLException {
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
