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

package gobblin.runtime;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.MysqlStateStore;
import gobblin.runtime.util.DatasetStateStoreUtils;


/**
 * A custom extension to {@link MysqlStateStore} for storing and reading {@link JobState.DatasetState}s.
 *
 * <p>
 *   The purpose of having this class is to hide some implementation details that are unnecessarily
 *   exposed if using the {@link MysqlStateStore} to store and serve job/dataset states between job runs.
 * </p>
 *
 * <p>
 *   In addition to persisting and reading {@link JobState.DatasetState}s. This class is also able to
 *   read job state files of existing jobs that store serialized instances of {@link JobState} for
 *   backward compatibility.
 * </p>
 *
 */
public class MysqlDatasetStateStore extends MysqlStateStore<JobState.DatasetState>
    implements DatasetStateStore<JobState.DatasetState> {

  private static final String SELECT_JOB_STATE_LATEST_BULK_TEMPLATE =
          "SELECT state, table_name FROM $TABLE$ WHERE store_name = ? and table_name IN (%s)";;

  private final String SELECT_JOB_STATE_LATEST_BULK_SQL;

  public MysqlDatasetStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, JobState.DatasetState.class);
    SELECT_JOB_STATE_LATEST_BULK_SQL = SELECT_JOB_STATE_LATEST_BULK_TEMPLATE.replace("$TABLE$", stateStoreTableName);
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(final String jobName) throws IOException {
    final Map<Optional<String>, String> latestDatasetStateTablesByUrn = getLatestDatasetStateTablesByUrn(jobName);
    StatementBuilder statementBuilder = new StatementBuilder() {
      @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
      @Override
      public PreparedStatement build(Connection connection) throws SQLException {
        int index = 1;
        String template = String.format(SELECT_JOB_STATE_LATEST_BULK_SQL,
                getInPredicate(latestDatasetStateTablesByUrn.size()));
        PreparedStatement queryStatement = connection.prepareStatement(template);
        queryStatement.setString(index++, jobName);
        for (String tableName : latestDatasetStateTablesByUrn.values()) {
          queryStatement.setString(index++, tableName);
        }
        return queryStatement;
      }
    };

    Map<String, JobState.DatasetState> datasetStatesByTableName =
        getAll(statementBuilder, new DatasetStatesByTableNameAccumulator());
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    for (Map.Entry<String, JobState.DatasetState> state : datasetStatesByTableName.entrySet()) {
      JobState.DatasetState previousDatasetState = state.getValue();
      previousDatasetState.setDatasetStateId(state.getKey());
      datasetStatesByUrns.put(previousDatasetState.getDatasetUrn(), previousDatasetState);
    }

    // The dataset (job) state from the deprecated "current.jst" will be read even though
    // the job has transitioned to the new dataset-based mechanism
    if (datasetStatesByUrns.size() > 1) {
      datasetStatesByUrns.remove(ConfigurationKeys.DEFAULT_DATASET_URN);
    }

    return datasetStatesByUrns;
  }

  /**
   * Get the latest {@link JobState.DatasetState} of a given dataset.
   *
   * @param storeName the name of the dataset state store
   * @param datasetUrn the dataset URN
   * @return the latest {@link JobState.DatasetState} of the dataset or {@link null} if it is not found
   * @throws IOException
   */
  public JobState.DatasetState getLatestDatasetState(String storeName, String datasetUrn) throws IOException {
    return DatasetStateStoreUtils.getLatestDatasetState(this, storeName, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    DatasetStateStoreUtils.persistDatasetState(this, datasetUrn, datasetState);
  }

  public Map<Optional<String>, String> getLatestDatasetStateTablesByUrn(String jobName) throws IOException {
    return DatasetStateStoreUtils.getLatestDatasetStateTablesByUrn(this, jobName);
  }

  private static String getInPredicate(int count) {
    return StringUtils.join(Iterables.limit(Iterables.cycle("?"), count).iterator(), ",");
  }

  private static class DatasetStatesByTableNameAccumulator
          implements ResultAccumulator<Map<String, JobState.DatasetState>, JobState.DatasetState> {
    private Map<String, JobState.DatasetState> states;

    @Override
    public void initialize() {
      states = Maps.newHashMap();
    }

    @Override
    public void add(@Nonnull ResultSet rs, JobState.DatasetState state) throws SQLException {
      String tableName = rs.getString(2);
      states.put(tableName, state);
    }

    @Nonnull
    @Override
    public Map<String, JobState.DatasetState> complete() {
      return states;
    }
  }
}
