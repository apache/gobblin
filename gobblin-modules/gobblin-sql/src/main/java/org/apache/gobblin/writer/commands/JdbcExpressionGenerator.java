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

package org.apache.gobblin.writer.commands;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.sql.JDBCType;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;


/**
 * Utility class for generating common expressions for JDBC.
 */
@AllArgsConstructor
public class JdbcExpressionGenerator {

	private static final String UPDATE_FILTER_TOKEN_PATTERN = "%s%s%s = %s";
	private static final Joiner AND_JOINER = Joiner.on(" AND ");
	private static final Joiner COMMA_JOINER = Joiner.on(", ");

	/** The character used to escape reserved names in the DB system. */
	private final String escapeChar;
	/** The character used to quote strings in the DB system. */
	private final String stringQuoteChar;

  /**
   * Generate a filter expression on the primary keys of a table.
   */
  public String generatePrimaryKeyFilter(JdbcEntryData data) {
		List<String> filters = new LinkedList<>();
		List<String> missingKeys = new LinkedList<>();
		for (String key : data.getSchema().getPrimaryKeys()) {
		  if (data.getVal(key) == null) {
		    missingKeys.add(key);
      } else {
        filters.add(String.format(UPDATE_FILTER_TOKEN_PATTERN, this.escapeChar, key, this.escapeChar,
            generateValue(data, key)));
      }
		}
		if (!missingKeys.isEmpty()) {
		  throw new IllegalArgumentException("Missing keys for key filter: " + COMMA_JOINER.join(missingKeys));
    }
		return AND_JOINER.join(filters);
	}

  /**
   * Generate an update statement for the non primary key columns of a table.
   */
	public String generateNonPrimaryKeyUpdateStatement(JdbcEntryData data) {
	  Set<String> primaryKeys = Sets.newHashSet(data.getSchema().getPrimaryKeys());
	  List<String> tokens = new LinkedList<>();
	  for (String column : data.getJdbcEntryData().keySet()) {
      if (!primaryKeys.contains(column)) {
        tokens.add(String.format(UPDATE_FILTER_TOKEN_PATTERN, this.escapeChar, column, this.escapeChar, generateValue(data, column)));
      }
    }
    return COMMA_JOINER.join(tokens);
  }

	private String generateValue(JdbcEntryData data, String column) {
		JDBCType tpe = data.getSchema().getJdbcType(column);
		switch (tpe) {
			case BIGINT:
			case INTEGER:
			case NUMERIC:
			case SMALLINT:
				return data.getVal(column).toString();
			case VARCHAR:
      case NVARCHAR:
      case LONGVARCHAR:
      case LONGNVARCHAR:
				return String.format("%s%s%s", this.stringQuoteChar, data.getVal(column).toString(), this.stringQuoteChar);
				default:
					throw new IllegalArgumentException(String.format("Type unsupported: %s -> %s", column, tpe));
		}
	}

}
