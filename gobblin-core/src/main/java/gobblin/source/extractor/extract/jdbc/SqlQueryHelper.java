/**
 *
 */
package gobblin.source.extractor.extract.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A helper class for handling SQL queries as strings. This is a temp solution until something more
 * robust is implemented.
 *
 * @see https://github.com/linkedin/gobblin/issues/236
 */
public class SqlQueryHelper {

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
