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
package gobblin.source.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

/**
 * A helper class for handling SQL queries as strings. This is a temp solution until something more
 * robust is implemented.
 *
 * @see https://github.com/linkedin/gobblin/issues/236
 */
public class SqlQueryUtils {

  /**
   * Add a new predicate(filter condition) to the query. The method will add a "where" clause if
   * none exists. Otherwise, it will add the condition as a conjunction (and).
   *
   * <b>Note that this method is rather limited. It works only if there are no other clauses after
   * "where"</b>
   *
   * @param query           the query string to modify
   * @param predicateCond   the predicate to add to the query
   * @return query          the new query string
   * @throws IllegalArgumentException if the predicate cannot be added because of additional clauses
   */
  public static String addPredicate(String query, String predicateCond) {
    if (Strings.isNullOrEmpty(predicateCond)) {
      return query;
    }
    String normalizedQuery = query.toLowerCase().trim();
    checkArgument(normalizedQuery.contains(" from "),
                  "query does not contain 'from': " + query);
    checkArgument(! normalizedQuery.contains(" by "),
                                "query contains 'order by' or 'group by': " + query);
    checkArgument(! normalizedQuery.contains(" having "),
                  "query contains 'having': " + query);
    checkArgument(! normalizedQuery.contains(" limit "),
                  "query contains 'limit': " + query);

    String keyword = " where ";
    if (normalizedQuery.contains(keyword)) {
      keyword = " and ";
    }
    query = query + keyword + "(" + predicateCond + ")";
    return query;
  }

  /**
   * Cast a string representation of a boolean value to a boolean primitive.
   * Used especially for Oracle representation of booleans as varchar2(1)
   * Returns true for values such as [t|true|yes|1] and false for [f|false|no].
   * If a boolean value cannot be trivially parsed, false is returned.
   *
   * @param fieldValue    the value of the boolean string field
   */
  public static boolean castToBoolean(String fieldValue) {
    String lowerField = fieldValue.toLowerCase();

    switch(lowerField) {
      case "y": return true;
      case "n": return false;
      case "true": return true;
      case "false": return false;
      case "t": return true;
      case "f": return false;
      case "yes": return true;
      case "no": return false;
      case "0": return false;
      case "1": return true;
    }
    return false;
  }

}