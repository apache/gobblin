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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.MysqlStateStore;
import gobblin.metastore.util.StateStoreTableInfo;
import gobblin.runtime.util.DatasetUrnSanitizer;


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

  private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDatasetStateStore.class);

  private static final String SELECT_JOB_STATE_LATEST_BY_PREFIX_TEMPLATE =
          "SELECT state, CASE WHEN locate(?, table_name) = 0 THEN '' "
                  + "ELSE substring(table_name, 1, locate(?, table_name) - 1) END, "
                  + "table_name FROM $TABLE$ WHERE store_name = ? "
                  + "AND table_name IN (SELECT max(table_name) FROM $TABLE$ "
                  + "WHERE store_name = ? GROUP BY CASE WHEN locate(?, table_name) = 0 THEN '' "
                  + "ELSE substring(table_name, 1, locate(?, table_name) - 1) END)";

  private final String SELECT_JOB_STATE_LATEST_BY_PREFIX_SQL;

  public MysqlDatasetStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, JobState.DatasetState.class);
    SELECT_JOB_STATE_LATEST_BY_PREFIX_SQL = SELECT_JOB_STATE_LATEST_BY_PREFIX_TEMPLATE.replace("$TABLE$", stateStoreTableName);
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(String jobName) throws IOException {
    final String storeName = jobName;
    StatementBuilder statementBuilder = new StatementBuilder() {
      @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
      @Override
      public PreparedStatement build(Connection connection) throws SQLException {
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_LATEST_BY_PREFIX_SQL);
        queryStatement.setString(1, StateStoreTableInfo.TABLE_PREFIX_SEPARATOR);
        queryStatement.setString(2, StateStoreTableInfo.TABLE_PREFIX_SEPARATOR);
        queryStatement.setString(3, storeName);
        queryStatement.setString(4, storeName);
        queryStatement.setString(5, StateStoreTableInfo.TABLE_PREFIX_SEPARATOR);
        queryStatement.setString(6, StateStoreTableInfo.TABLE_PREFIX_SEPARATOR);
        return queryStatement;
      }
    };

    Map<Optional<String>, Pair<String, JobState.DatasetState>> latestDatasetStatesByUrns =
            getAll(statementBuilder, new LatestDatasetStatesByUrnsAccumulator());
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    for (Map.Entry<Optional<String>, Pair<String, JobState.DatasetState>> state : latestDatasetStatesByUrns.entrySet()) {
      JobState.DatasetState previousDatasetState = state.getValue().getValue();
      previousDatasetState.setDatasetStateId(state.getValue().getKey());
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
    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
        sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + StateStoreTableInfo.CURRENT_NAME :
        StateStoreTableInfo.CURRENT_NAME) + DATASET_STATE_STORE_TABLE_SUFFIX;
    return get(storeName, tableName, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    String jobName = datasetState.getJobName();
    String jobId = datasetState.getJobId();

    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
        sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + jobId : jobId) +
        DATASET_STATE_STORE_TABLE_SUFFIX;
    LOGGER.info("Persisting " + tableName + " to the job state store");
    put(jobName, tableName, datasetState);
  }

  private static class LatestDatasetStatesByUrnsAccumulator
          implements ResultAccumulator<Map<Optional<String>, Pair<String, JobState.DatasetState>>, JobState.DatasetState> {
    private Map<Optional<String>, Pair<String, JobState.DatasetState>> states;

    @Override
    public void initialize() {
      states = Maps.newHashMap();
    }

    @Override
    public void add(@Nonnull ResultSet rs, JobState.DatasetState state) throws SQLException {
      String urn = rs.getString(2);
      String tableName = rs.getString(3);
      states.put(Optional.fromNullable(urn), Pair.of(tableName, state));
    }

    @Nonnull
    @Override
    public Map<Optional<String>, Pair<String, JobState.DatasetState>> complete() {
      return states;
    }
  }
}
