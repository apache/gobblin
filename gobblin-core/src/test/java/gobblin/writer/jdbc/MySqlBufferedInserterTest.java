package gobblin.writer.jdbc;

import static org.mockito.Mockito.*;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;
import gobblin.writer.commands.MySqlBufferedInserter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.writer"})
public class MySqlBufferedInserterTest {

  public void testMySqlBufferedInsert() throws SQLException {
    final String table = "stg";
    final int colNums = 20;
    final int batchSize = 100;
    final int entryCount = 1007;
    final int colSize = 7;

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));
    MySqlBufferedInserter inserter = new MySqlBufferedInserter(state);

    Connection conn = mock(Connection.class);
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for(JdbcEntryData entry : jdbcEntries) {
      inserter.insert(conn, table, entry);
    }
    inserter.flush(conn);

    verify(conn, times(2)).prepareStatement(anyString());
    verify(pstmt, times(11)).clearParameters();
    verify(pstmt, times(11)).execute();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
  }

  public void testMySqlBufferedInsertBufferSizeLimit() throws SQLException {
    final String table = "stg";
    final int colNums = 20;
    final int batchSize = 100;
    final int entryCount = 1007;
    final int colSize = 10;
    final int entrySize = RandomStringUtils.randomAlphabetic(colSize).toString().getBytes().length * colNums;
    final double bufferRatio = 0.8D;
    final int bufferSize = (int) (entrySize * batchSize * bufferRatio);

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));
    state.setProp(ConfigurationKeys.WRITER_JDBC_INSERT_BUFFER_SIZE, Integer.toString(bufferSize));

    MySqlBufferedInserter inserter = new MySqlBufferedInserter(state);

    Connection conn = mock(Connection.class);
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for(JdbcEntryData entry : jdbcEntries) {
      inserter.insert(conn, table, entry);
    }
    inserter.flush(conn);

    int expectedBatchSize = (int) (batchSize * bufferRatio);
    int expectedExecuteCount = entryCount / expectedBatchSize + 1;
    verify(conn, times(expectedExecuteCount + 1)).prepareStatement(anyString());
    verify(pstmt, times(expectedExecuteCount)).clearParameters();
    verify(pstmt, times(expectedExecuteCount)).execute();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
  }

  public void testMySqlBufferedInsertParamLimit() throws SQLException {
    final String table = "stg";
    final int colNums = 2000;
    final int batchSize = 100;
    final int entryCount = 1007;
    final int colSize = 3;

    State state = new State();
    state.setProp(ConfigurationKeys.WRITER_JDBC_INSERT_BATCH_SIZE, Integer.toString(batchSize));

    MySqlBufferedInserter inserter = new MySqlBufferedInserter(state);

    Connection conn = mock(Connection.class);
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(pstmt);

    List<JdbcEntryData> jdbcEntries = createJdbcEntries(colNums, colSize, entryCount);
    for(JdbcEntryData entry : jdbcEntries) {
      inserter.insert(conn, table, entry);
    }
    inserter.flush(conn);

    int expectedBatchSize = ConfigurationKeys.DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE / colNums;
    int expectedExecuteCount = entryCount / expectedBatchSize + 1;
    verify(conn, times(2)).prepareStatement(anyString());
    verify(pstmt, times(expectedExecuteCount)).clearParameters();
    verify(pstmt, times(expectedExecuteCount)).execute();
    verify(pstmt, times(colNums * entryCount)).setObject(anyInt(), anyObject());
  }

  private List<JdbcEntryData> createJdbcEntries(int colNums, int colSize, int entryCount) {
    Set<String> colNames = new HashSet<>();
    while (colNames.size() < colNums) {
      String colName = RandomStringUtils.randomAlphabetic(colSize);
      if (colNames.contains(colName)) {
        continue;
      }
      colNames.add(colName);
    }

    List<JdbcEntryData> result = new ArrayList<>();
    for (int i = 0; i < entryCount; i++) {
      result.add(createJdbcEntry(colNames, colSize));
    }
    return result;
  }

  private JdbcEntryData createJdbcEntry(Collection<String> colNames, int colSize) {
    List<JdbcEntryDatum> datumList = new ArrayList<>();
    for (String colName : colNames) {
      JdbcEntryDatum datum = new JdbcEntryDatum(colName, RandomStringUtils.randomAlphabetic(colSize));
      datumList.add(datum);
    }
    return new JdbcEntryData(datumList);
  }
}
