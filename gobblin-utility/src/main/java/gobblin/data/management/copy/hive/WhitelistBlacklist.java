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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;


/**
 * A whitelist / blacklist implementation for filtering Hive tables. Parses input whitelist and blacklist of the form
 * [dbpattern.tablepattern1|tablepattern2|...],... and filters accordingly. The db and table patterns accept "*"
 * characters. Each of whitelist and blacklist is a list of patterns. For a table to be accepted, it must fail the
 * blacklist filter and pass the whitelist filter. Empty whitelist or blacklist are noops.
 *
 * <p>
 *   Example whitelist and blacklist patterns:
 *   * db1.table1 -> only db1.table1 passes.
 *   * db1 -> any table under db1 passes.
 *   * db1.table* -> any table under db1 whose name satisfies the pattern table* passes.
 *   * db* -> all tables from all databases whose names satisfy the pattern db* pass.
 *   * db*.table* -> db and table must satisfy the patterns db* and table* respectively
 *   * db1.table1,db2.table2 -> combine expressions for different databases with comma.
 *   * db1.table1|table2 -> combine expressions for same database with "|".
 * </p>
 */
public class WhitelistBlacklist implements Serializable {

  public static final String WHITELIST = "whitelist";
  public static final String BLACKLIST = "blacklist";

  private static final Pattern ALL_TABLES = Pattern.compile(".*");

  private final SetMultimap<Pattern, Pattern> whitelistMultimap;
  private final SetMultimap<Pattern, Pattern> blacklistMultimap;

  public WhitelistBlacklist(Config config) throws IOException {
    this(config.hasPath(WHITELIST) ? config.getString(WHITELIST).toLowerCase() : "",
        config.hasPath(BLACKLIST) ? config.getString(BLACKLIST).toLowerCase() : "");
  }

  public WhitelistBlacklist(String whitelist, String blacklist) throws IOException {
    this.whitelistMultimap = HashMultimap.create();
    this.blacklistMultimap = HashMultimap.create();

    populateMultimap(this.whitelistMultimap, whitelist.toLowerCase());
    populateMultimap(this.blacklistMultimap, blacklist.toLowerCase());
  }

  /**
   * @return Whether database db might contain tables accepted by this {@link WhitelistBlacklist}.
   */
  public boolean acceptDb(String db) {
    return accept(db, Optional.<String> absent());
  }

  /**
   * @return Whether the input table is accepted by this {@link WhitelistBlacklist}.
   */
  public boolean acceptTable(String db, String table) {
    return accept(db.toLowerCase(), table==null? Optional.<String> absent(): Optional.fromNullable(table.toLowerCase()));
  }

  private boolean accept(String db, Optional<String> table) {
    if (!this.blacklistMultimap.isEmpty() && multimapContains(this.blacklistMultimap, db, table, true)) {
      return false;
    }

    return this.whitelistMultimap.isEmpty() || multimapContains(this.whitelistMultimap, db, table, false);
  }

  private static void populateMultimap(SetMultimap<Pattern, Pattern> multimap, String list) throws IOException {
    Splitter tokenSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
    Splitter partSplitter = Splitter.on(".").omitEmptyStrings().trimResults();
    Splitter tableSplitter = Splitter.on("|").omitEmptyStrings().trimResults();

    for (String token : tokenSplitter.split(list)) {

      if (!Strings.isNullOrEmpty(token)) {
        List<String> parts = partSplitter.splitToList(token);
        if (parts.size() > 2) {
          throw new IOException("Invalid token " + token);
        }

        Pattern databasePattern = Pattern.compile(parts.get(0).replace("*", ".*"));
        Set<Pattern> tablePatterns = Sets.newHashSet();
        if (parts.size() == 2) {
          String tables = parts.get(1);
          for (String table : tableSplitter.split(tables)) {
            if (table.equals("*")) {
              // special case, must use ALL_TABLES due to use of set.contains(ALL_TABLES) in multimapContains
              tablePatterns.add(ALL_TABLES);
            } else {
              tablePatterns.add(Pattern.compile(table.replace("*", ".*")));
            }
          }
        } else {
          tablePatterns.add(ALL_TABLES);
        }
        multimap.putAll(databasePattern, tablePatterns);
      }
    }
  }

  private static boolean multimapContains(SetMultimap<Pattern, Pattern> multimap, String database,
      Optional<String> table, boolean blacklist) {
    for (Pattern dbPattern : multimap.keySet()) {
      if (dbPattern.matcher(database).matches()) {
        if (!table.isPresent()) {
          // if we are only matching database
          return !blacklist || multimap.get(dbPattern).contains(ALL_TABLES);
        }

        for (Pattern tablePattern : multimap.get(dbPattern)) {
          if (tablePattern.matcher(table.get()).matches()) {
            return true;
          }
        }
      }
    }

    return false;
  }
}
