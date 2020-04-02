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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.metastore.predicates.StoreNamePredicate;


@Slf4j
/**
 * An implementation of {@link MysqlStateStore} backed by MySQL to store JobStatuses.
 *
 * @param <T> state object type
 **/
public class MysqlJobStatusStateStore<T extends State> extends MysqlStateStore<T> implements DatasetStateStore<T> {
  /**
   * Manages the persistence and retrieval of {@link State} in a MySQL database
   * @param dataSource the {@link DataSource} object for connecting to MySQL
   * @param stateStoreTableName the table for storing the state in rows keyed by two levels (store_name, table_name)
   * @param compressedValues should values be compressed for storage?
   * @param stateClass class of the {@link State}s stored in this state store
   * @throws IOException in case of failures
   */
  public MysqlJobStatusStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues,
      Class<T> stateClass)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, stateClass);
  }

  /**
   * Returns all the job statuses for a flow group, flow name, flow execution id
   * @param storeName store name in the state store
   * @param flowExecutionId Flow Execution Id
   * @return list of states
   * @throws IOException in case of failures
   */
  public List<T> getAll(String storeName, long flowExecutionId) throws IOException {
    return getAll(storeName, flowExecutionId + "%", true);
  }

  @Override
  public List<DatasetStateStoreEntryManager<T>> getMetadataForTables(StateStorePredicate predicate)
      throws IOException {
    List<DatasetStateStoreEntryManager<T>> entryManagers = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_METADATA_SQL)) {
      String storeName = predicate instanceof StoreNamePredicate ? ((StoreNamePredicate) predicate).getStoreName() : "%";
      queryStatement.setString(1, storeName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          String rsStoreName = rs.getString(1);
          String rsTableName = rs.getString(2);
          Timestamp timestamp = rs.getTimestamp(3);

          DatasetStateStoreEntryManager<T> entryManager =
              new MysqlJobStatusStateStoreEntryManager<>(rsStoreName, rsTableName, timestamp.getTime(), this);

          if (predicate.apply(entryManager)) {
            entryManagers.add(new MysqlJobStatusStateStoreEntryManager<T>(rsStoreName, rsTableName, timestamp.getTime(), this));
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException("failure getting metadata for tables", e);
    }

    return entryManagers;
  }

  @Override
  public Map<String, T> getLatestDatasetStatesByUrns(String jobName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T getLatestDatasetState(String storeName, String datasetUrn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persistDatasetState(String datasetUrn, T datasetState) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persistDatasetURNs(String storeName, Collection<String> datasetUrns) {
    throw new UnsupportedOperationException();
  }
}
