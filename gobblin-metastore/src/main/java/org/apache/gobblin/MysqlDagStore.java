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

package org.apache.gobblin;

import java.io.IOException;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.service.ServiceConfigKeys;


@Slf4j
/**
 * An implementation of {@link MysqlStateStore} backed by MySQL to store Dag.
 *
 * @param <T> state object type
 **/
public class MysqlDagStore<T extends State> extends MysqlStateStore<T> {
  /**
   * Manages the persistence and retrieval of {@link State} in a MySQL database
   * @param dataSource the {@link DataSource} object for connecting to MySQL
   * @param stateStoreTableName the table for storing the state in rows keyed by two levels (store_name, table_name)
   * @param compressedValues should values be compressed for storage?
   * @param stateClass class of the {@link State}s stored in this state store
   * @throws IOException in case of failures
   */
  public MysqlDagStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues,
      Class<T> stateClass)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, stateClass);
  }

  @Override
  protected String getCreateJobStateTableTemplate() {
    int maxStoreName = ServiceConfigKeys.MAX_FLOW_NAME_LENGTH + ServiceConfigKeys.STATE_STORE_KEY_SEPARATION_CHARACTER.length()
        + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH;
    int maxTableName = ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH;

    return "CREATE TABLE IF NOT EXISTS $TABLE$ (store_name varchar(" + maxStoreName + ") CHARACTER SET latin1 COLLATE latin1_bin not null,"
        + "table_name varchar(" + maxTableName + ") CHARACTER SET latin1 COLLATE latin1_bin not null,"
        + " modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        + " state longblob, primary key(store_name, table_name))";
  }
}
