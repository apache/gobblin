package gobblin.converter.jdbc;

import java.util.Objects;

public class JdbcEntryDatum {
  private final String columnName;
  private final Object val;
  private int byteSize;

  public JdbcEntryDatum(String columnName, Object val) {
    this.columnName = Objects.requireNonNull(columnName);
    this.val = Objects.requireNonNull(val);
    this.byteSize = val.toString().getBytes().length;
  }

  public String getColumnName() {
    return columnName;
  }

  public Object getVal() {
    return val;
  }

  /**
   * @return Size in terms of bytes, when it's converted into String.
   */
  public int getByteSize() {
    return byteSize;
  }

  /**
   * Note that it only uses column name as a key
   * {@inheritDoc}
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
    return result;
  }

  /**
   * Note that it only uses column name as a key
   * {@inheritDoc}
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JdbcEntryDatum other = (JdbcEntryDatum) obj;
    if (columnName == null) {
      if (other.columnName != null)
        return false;
    } else if (!columnName.equals(other.columnName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format("JdbcEntry [columnName=%s, val=%s]", columnName, val);
  }

}
