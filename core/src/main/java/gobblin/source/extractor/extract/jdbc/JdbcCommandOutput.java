/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import gobblin.source.extractor.extract.CommandOutput;


/**
 * Captures output of a JDBC command, keep track of commands and its outputs
 *
 * @author nveeramr
 */
public class JdbcCommandOutput implements CommandOutput<JdbcCommand, ResultSet> {
  private Map<JdbcCommand, ResultSet> results;

  public JdbcCommandOutput() {
    results = new HashMap<JdbcCommand, ResultSet>();
  }

  @Override
  public void storeResults(Map<JdbcCommand, ResultSet> results) {
    this.results = results;
  }

  @Override
  public Map<JdbcCommand, ResultSet> getResults() {
    return results;
  }

  @Override
  public void put(JdbcCommand key, ResultSet value) {
    results.put(key, value);
  }
}