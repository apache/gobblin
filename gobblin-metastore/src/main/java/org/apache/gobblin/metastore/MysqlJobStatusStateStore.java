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

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;

@Slf4j
/**
 * An implementation of {@link MysqlStateStore} backed by MySQL to store JobStatuses.
 *
 * @param <T> state object type
 **/
public class MysqlJobStatusStateStore<T extends State> extends MysqlStateStore {
  /**
   * Manages the persistence and retrieval of {@link State} in a MySQL database
   * @param dataSource the {@link DataSource} object for connecting to MySQL
   * @param stateStoreTableName the table for storing the state in rows keyed by two levels (store_name, table_name)
   * @param compressedValues should values be compressed for storage?
   * @param stateClass class of the {@link State}s stored in this state store
   * @throws IOException
   */
  public MysqlJobStatusStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues,
      Class<T> stateClass)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, stateClass);
  }

  /**
   * Returns all the job statuses for a flow group, flow name, flow execution id
   * @param storeName
   * @param flowExecutionId
   * @return
   * @throws IOException
   */
  public List<T> getAll(String storeName, long flowExecutionId) throws IOException {
    return getAll(storeName, flowExecutionId + "%", true);
  }
}
