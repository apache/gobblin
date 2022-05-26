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

package org.apache.gobblin.util.jdbc;

public final class MysqlDataSourceUtils {
  /**
   *  This query will validate that MySQL connection is active and Mysql instance is writable.
   *
   *  If a database failover happened, and current replica became read-only, this query will fail and
   *  connection will be removed from the pool.
   *
   *  See https://stackoverflow.com/questions/39552146/evicting-connections-to-a-read-only-node-in-a-cluster-from-the-connection-pool
   * */
  public static final String QUERY_CONNECTION_IS_VALID_AND_NOT_READONLY =
      "select case when @@read_only = 0 then 1 else (select table_name from information_schema.tables) end as `1`";

  private MysqlDataSourceUtils() {
  }
}
