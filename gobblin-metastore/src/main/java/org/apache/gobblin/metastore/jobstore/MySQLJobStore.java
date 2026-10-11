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

package org.apache.gobblin.metastore.jobstore;

import com.linkedin.data.template.StringMap;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.rest.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLJobStore implements JobStore {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLJobStore.class);

    private DataSource dataSource;

    private final String SELECT_JOB_STATEMENT = "SELECT * from gobblin_jobs where name=?";
    private final String INSERT_JOB_STATEMENT = "INSERT INTO gobblin_jobs (name, description, schedule, is_disabled, " +
            "priority, configs, owner_email, source_system, target_system) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private final String UPDATE_JOB_STATMENT = "UPDATE gobblin_jobs set description=?, schedule=?, is_disabled=?, " +
            "priority=?, configs=?, owner_email=?, source_system=?, target_system=? where name=?";
    private final String DELETE_JOB_TEMPLATE = "DELETE from gobblin_jobs where name=?";

    @Inject
    public MySQLJobStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Job create(Job job) throws Exception {
        Preconditions.checkArgument(job.hasName());

        PreparedStatement insertStatement = getNewPreparedStatement(INSERT_JOB_STATEMENT);
        insertStatement.setString(1, job.getName());
        insertStatement.setString(2, job.getDescription());
        insertStatement.setString(3, job.getSchedule());
        insertStatement.setBoolean(4, job.isDisabled());
        insertStatement.setInt(5, job.getPriority());

        Config config = ConfigFactory.parseMap(job.getConfigs());
        insertStatement.setString(6, config.root().render(ConfigRenderOptions.concise()));
        insertStatement.setString(7, job.getOwnerEmail());
        insertStatement.setString(8, job.getSourceSystem());
        insertStatement.setString(9, job.getTargetSystem());

        this.executeUpdate(insertStatement);
        LOG.info("created new job:" + job.getName());
        return job;
    }

    @Override
    public Job get(String jobName) throws Exception {
        Preconditions.checkArgument(StringUtils.isNotBlank(jobName));

        PreparedStatement selectStatment = getNewPreparedStatement(SELECT_JOB_STATEMENT);
        selectStatment.setString(1, jobName.trim());
        ResultSet jobResultSet = executeQuery(selectStatment);
        Job job = new Job();

        if (jobResultSet != null && jobResultSet.next()) {
            job.setId(jobResultSet.getInt("id"));
            job.setName(jobResultSet.getString("name"));
            job.setDescription(jobResultSet.getString("description"));
            job.setSchedule(jobResultSet.getString("schedule"));
            job.setDisabled(jobResultSet.getBoolean("is_disabled"));
            job.setPriority(jobResultSet.getInt("priority"));

            String configString = jobResultSet.getString("configs");
            StringMap jobConfigMap = new StringMap();
            if (StringUtils.isNotBlank(configString)) {
                Config config = ConfigFactory.parseString(jobResultSet.getString("configs"));
                config.entrySet().forEach(e -> jobConfigMap.put(e.getKey(), e.getValue().unwrapped().toString()));
            }
            job.setConfigs(jobConfigMap);
            job.setSourceSystem(jobResultSet.getString("source_system"));
            job.setTargetSystem(jobResultSet.getString("target_system"));
            job.setOwnerEmail(jobResultSet.getString("owner_email"));
            job.setCreatedDate(jobResultSet.getTimestamp("created_date").getTime());
            job.setUpdatedDate(jobResultSet.getTimestamp("updated_date").getTime());
        }

        return job;
    }

    Job mergeJob(Job existingJob, Job newJob) {
        // id and Name can not be changed.
        if (StringUtils.isNotBlank(newJob.getDescription())) {
            existingJob.setDescription(newJob.getDescription());
        }
        if (newJob.isDisabled()) {
            existingJob.setDisabled(newJob.isDisabled());
        }
        if (StringUtils.isNotBlank(newJob.getSchedule())) {
            existingJob.setSchedule(newJob.getSchedule());
        }
        if (newJob.getPriority() != null) {
            existingJob.setPriority(newJob.getPriority());
        }
        if (StringUtils.isNotBlank(newJob.getOwnerEmail())) {
            existingJob.setOwnerEmail(newJob.getOwnerEmail());
        }
        if (StringUtils.isNotBlank(newJob.getSourceSystem())) {
            existingJob.setSourceSystem(newJob.getSourceSystem());
        }
        if (StringUtils.isNotBlank(newJob.getTargetSystem())) {
            existingJob.setTargetSystem(newJob.getTargetSystem());
        }

        // handle config to be removed
        if (newJob.getConfigsToRemove() != null) {
            newJob.getConfigsToRemove().forEach(configKeyToRemove -> existingJob.getConfigs().remove(configKeyToRemove));
        }

        // merge the configs
        Config existingConfig = ConfigFactory.parseMap(existingJob.getConfigs());
        Config newConfig = ConfigFactory.parseMap(newJob.getConfigs());
        Config mergedConfig = existingConfig.withFallback(newConfig);
        StringMap jobConfigMap = new StringMap();
        mergedConfig.entrySet().forEach(e -> jobConfigMap.put(e.getKey(), e.getValue().unwrapped().toString()));

        existingJob.setConfigs(jobConfigMap);
        return existingJob;

    }

    @Override
    public boolean update(Job job) throws Exception {
        Preconditions.checkNotNull(job);
        Preconditions.checkNotNull(job.getName());

        PreparedStatement updateStatement = getNewPreparedStatement(UPDATE_JOB_STATMENT);
        updateStatement.setString(1, job.getDescription());
        updateStatement.setString(2, job.getSchedule());
        updateStatement.setBoolean(3, job.isDisabled());
        updateStatement.setInt(4, job.getPriority());
        Config config = ConfigFactory.parseMap(job.getConfigs());
        updateStatement.setString(5, config.root().render(ConfigRenderOptions.concise()));
        updateStatement.setString(6, job.getOwnerEmail());
        updateStatement.setString(7, job.getSourceSystem());
        updateStatement.setString(8, job.getTargetSystem());
        updateStatement.setString(9, job.getName());
        this.executeUpdate(updateStatement);
        return true;
    }

    @Override
    public boolean mergedUpdate(final Job newJob) throws Exception {
        Preconditions.checkNotNull(newJob);
        Preconditions.checkNotNull(newJob.getName());

        final Job job = get(newJob.getName());
        mergeJob(job, newJob);
        return update(job);
    }


    @Override
    public boolean delete(String jobName) throws Exception {
        Preconditions.checkArgument(StringUtils.isNotBlank(jobName));
        PreparedStatement deleteStatement = getNewPreparedStatement(DELETE_JOB_TEMPLATE);
        deleteStatement.setString(1, jobName);
        return this.executeUpdate(deleteStatement);
    }

    @SuppressFBWarnings(
            value = "ODR_OPEN_DATABASE_RESOURCE",
            justification = "The connection is always closed once the statement is executed.")
    private PreparedStatement getNewPreparedStatement(String sql) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            connection.setAutoCommit(false);
            return connection.prepareStatement(sql);
        } catch (SQLException se) {
            LOG.error("Failed to create SQLStatement: " + sql, se);
            LOG.error("Closing the connection...");
            if (connection != null && !connection.isClosed()) {
                try {
                    connection.close();
                    LOG.debug("connection closed.");
                } catch (SQLException e) {
                    LOG.error("Failed to close connection.", e);
                }
            }
            throw se;
        }
    }

    private boolean executeUpdate(PreparedStatement preparedStatement) throws SQLException {
        return executeUpdate(preparedStatement, false);
    }

    private boolean executeBatchUpdate(PreparedStatement preparedStatement) throws SQLException {
        return executeUpdate(preparedStatement, true);
    }

    private boolean executeUpdate(PreparedStatement preparedStatement, boolean isBatch) throws SQLException {
        try {
            if (isBatch) {
                int[] batchUpdateResults = preparedStatement.executeBatch();
                for (int batchUpdateResultCode : batchUpdateResults) {
                    if (batchUpdateResultCode == 0) {
                        return false;
                    }
                }
                return true;
            } else {
                return preparedStatement.executeUpdate() > 0;
            }
        } catch (SQLException se) {
            LOG.error("Failed to execute SQLStatement : " + preparedStatement.toString(), se);
            throw se;
        } finally {
            // commitNcloseConnectionForStatement
            try {
                Connection connection = preparedStatement.getConnection();
                if (connection != null && !connection.isClosed()) {
                    try {
                        connection.commit();
                        connection.close();
                        LOG.debug("connection closed for statement:" + preparedStatement.toString());
                    } catch (SQLException se) {
                        connection.rollback();
                        LOG.error("Failed to close connection for statement:" + preparedStatement.toString(), se);
                    }
                }
            } catch (SQLException se) {
                LOG.error("Failed to commit or close the connection for SQLStatement : " + preparedStatement.toString(), se);
            }
        }
    }

    private ResultSet executeQuery(PreparedStatement preparedStatement) throws SQLException {
        try {
            return preparedStatement.executeQuery();
        } catch (SQLException se) {
            LOG.error("Failed to execute SQLStatement : " + preparedStatement.toString(), se);
            throw se;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.dataSource != null
                    && this.dataSource.getConnection() != null
                    && !this.dataSource.getConnection().isClosed()) {
                this.dataSource.getConnection().close();
            }
        } catch (SQLException se) {
            LOG.error("Failed to close the connection", se);
        }
    }

}
