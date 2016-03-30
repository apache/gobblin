/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.jdbc;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

public class JdbcEntryData implements Iterable<JdbcEntryDatum> {
  private final Map<String, JdbcEntryDatum> jdbcEntryData; //Pair of column name and Object
  private final int byteSize;

  public JdbcEntryData(Iterable<JdbcEntryDatum> jdbcEntryDatumEntries) {
    Preconditions.checkNotNull(jdbcEntryDatumEntries);
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
