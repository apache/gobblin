package gobblin.converter.jdbc;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

public class JdbcEntryData implements Iterable<JdbcEntryDatum> {
  private final Map<String, JdbcEntryDatum> jdbcEntryData; //Pair of column name and Object
  private final int byteSize;

  public JdbcEntryData(Iterable<JdbcEntryDatum> jdbcEntryDatumEntries) {
    Objects.requireNonNull(jdbcEntryDatumEntries);
    ImmutableMap.Builder<String, JdbcEntryDatum> builder = ImmutableSortedMap.naturalOrder();
    int byteCount = 0;
    for (JdbcEntryDatum datum : jdbcEntryDatumEntries) {
      builder.put(datum.getColumnName(), datum);
      byteCount += datum.getByteSize();
    }
    this.jdbcEntryData = builder.build();
    this.byteSize = byteCount;
  }

  /**
   * @param columnName Column name case sensitive, as most of RDBMS does.
   * @return Returns Object which is JDBC compatible -- can be used for PreparedStatement.setObject
   */
  public Object getVal(String columnName) {
    JdbcEntryDatum datum = jdbcEntryData.get(columnName);
    return datum == null ? null : datum.getVal();
  }

  @Override
  public String toString() {
    return String.format("JdbcEntryData [jdbcEntryData=%s]", jdbcEntryData);
  }

  /**
   * Provides iterator sorted by column name
   * {@inheritDoc}
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<JdbcEntryDatum> iterator() {
    return jdbcEntryData.values().iterator();
  }

  public int byteSize() {
    return byteSize;
  }
}
