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
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

@ToString
@EqualsAndHashCode
public class JdbcEntrySchema implements Iterable<JdbcEntryMetaDatum> {
  private final Map<String, JdbcEntryMetaDatum> jdbcMetaData; //Pair of column name and JdbcType

  public JdbcEntrySchema(Iterable<JdbcEntryMetaDatum> jdbcMetaDatumEntries) {
    Preconditions.checkNotNull(jdbcMetaDatumEntries);
    ImmutableMap.Builder<String, JdbcEntryMetaDatum> builder = ImmutableSortedMap.naturalOrder();
    for (JdbcEntryMetaDatum datum : jdbcMetaDatumEntries) {
      builder.put(datum.getColumnName(), datum);
    }
    jdbcMetaData = builder.build();
  }

  /**
   * @param columnName Column name case sensitive, as most of RDBMS does.
   * @return Returns JdbcType. If column name does not exist, returns null.
   */
  public JdbcType getJdbcType(String columnName) {
    JdbcEntryMetaDatum datum = jdbcMetaData.get(columnName);
    return datum == null ? null : datum.getJdbcType();
  }

  public Set<String> getColumnNames() {
    return jdbcMetaData.keySet();
  }

  /**
   * Provides iterator sorted by column name
   * {@inheritDoc}
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<JdbcEntryMetaDatum> iterator() {
    return jdbcMetaData.values().iterator();
  }
}
