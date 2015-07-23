/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.source.extractor.extract.jdbc;

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

}
