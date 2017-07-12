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

package gobblin.converter.jdbc;

import java.util.Iterator;
import java.util.Map;

import lombok.ToString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;


@ToString
public class JdbcEntryData implements Iterable<JdbcEntryDatum> {
  private final Map<String, JdbcEntryDatum> jdbcEntryData; //Pair of column name and Object

  public JdbcEntryData(Iterable<JdbcEntryDatum> jdbcEntryDatumEntries) {
    Preconditions.checkNotNull(jdbcEntryDatumEntries);
    ImmutableMap.Builder<String, JdbcEntryDatum> builder = ImmutableSortedMap.naturalOrder();
    for (JdbcEntryDatum datum : jdbcEntryDatumEntries) {
      builder.put(datum.getColumnName(), datum);
    }
    this.jdbcEntryData = builder.build();
  }

  /**
   * @param columnName Column name case sensitive, as most of RDBMS does.
   * @return Returns Object which is JDBC compatible -- can be used for PreparedStatement.setObject
   */
  public Object getVal(String columnName) {
    JdbcEntryDatum datum = this.jdbcEntryData.get(columnName);
    return datum == null ? null : datum.getVal();
  }

  /**
   * Provides iterator sorted by column name
   * {@inheritDoc}
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<JdbcEntryDatum> iterator() {
    return this.jdbcEntryData.values().iterator();
  }
}
