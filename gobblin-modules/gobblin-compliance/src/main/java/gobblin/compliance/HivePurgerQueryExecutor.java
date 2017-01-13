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
package gobblin.compliance;

import java.sql.SQLException;
import java.util.List;

import gobblin.util.HiveJdbcConnector;


/**
 * This class is responsible for executing Hive queries by initializing {@link HiveJdbcConnector}
 *
 * @author adsharma
 */
public class HivePurgerQueryExecutor {
  private final HiveJdbcConnector hiveJdbcConnector;
  private static final int HIVE_SERVER_VERSION = 2;

  public HivePurgerQueryExecutor()
      throws SQLException {
    this.hiveJdbcConnector = HiveJdbcConnector.newEmbeddedConnector(HIVE_SERVER_VERSION);
  }

  public void executeQueries(List<String> queries)
      throws SQLException {
    this.hiveJdbcConnector.executeStatements(queries.toArray(new String[queries.size()]));
  }

  public void executeQuery(String query)
      throws SQLException {
    this.hiveJdbcConnector.executeStatements(query);
  }
}
