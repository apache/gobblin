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

package gobblin.metastore.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.linkedin.data.template.StringMap;

import gobblin.metastore.DatabaseJobHistoryStore;
import gobblin.metastore.JobHistoryStore;
import gobblin.rest.JobExecutionInfo;
import gobblin.rest.JobExecutionQuery;
import gobblin.rest.JobStateEnum;
import gobblin.rest.LauncherTypeEnum;
import gobblin.rest.Metric;
import gobblin.rest.MetricArray;
import gobblin.rest.MetricTypeEnum;
import gobblin.rest.QueryListType;
import gobblin.rest.Table;
import gobblin.rest.TableTypeEnum;
import gobblin.rest.TaskExecutionInfo;
import gobblin.rest.TaskExecutionInfoArray;
import gobblin.rest.TaskStateEnum;
import gobblin.rest.TimeRange;


/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *   The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author Yinan Li
 */
@SupportedDatabaseVersion(isDefault = true, version = "1.0.0")
public class DatabaseJobHistoryStoreV100 implements VersionedDatabaseJobHistoryStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseJobHistoryStore.class);

  private static final String JOB_EXECUTION_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_executions (job_name,job_id,start_time,end_time,duration,state,"
          + "launched_tasks,completed_tasks,launcher_type,tracking_url) VALUES(?,?,?,?,?,?,?,?,?,?)";

  private static final String TASK_EXECUTION_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_executions (task_id,job_id,start_time,end_time,duration,"
          + "state,failure_exception,low_watermark,high_watermark,table_namespace,table_name,table_type) "
          + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

  private static final String JOB_METRIC_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_metrics (job_id,metric_group,metric_name,"
          + "metric_type,metric_value) VALUES(?,?,?,?,?)";

  private static final String TASK_METRIC_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_metrics (task_id,metric_group,metric_name,"
          + "metric_type,metric_value) VALUES(?,?,?,?,?)";

  private static final String JOB_PROPERTY_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_properties (job_id,property_key,property_value) VALUES(?,?,?)";

  private static final String TASK_PROPERTY_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_properties (task_id,property_key,property_value) VALUES(?,?,?)";

  private static final String JOB_EXECUTION_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_job_executions SET start_time=?,end_time=?,duration=?,"
          + "state=?,launched_tasks=?,completed_tasks=?,launcher_type=?,tracking_url=? WHERE job_id=?";

  private static final String TASK_EXECUTION_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_task_executions SET start_time=?,end_time=?,duration=?,state=?,failure_exception=?,"
          + "low_watermark=?,high_watermark=?,table_namespace=?,table_name=?,table_type=? WHERE task_id=?";

  private static final String JOB_METRIC_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_job_metrics SET metric_value=? WHERE job_id=? AND "
          + "metric_group=? AND metric_name=? AND metric_type=?";

  private static final String TASK_METRIC_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_task_metrics SET metric_value=? WHERE task_id=? AND "
          + "metric_group=? AND metric_name=? AND metric_type=?";

  private static final String JOB_PROPERTY_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_job_properties SET property_value=? WHERE job_id=? AND property_key=?";

  private static final String TASK_PROPERTY_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_task_properties SET property_value=? WHERE task_id=? AND property_key=?";

  private static final String LIST_DISTINCT_JOB_EXECUTION_QUERY_TEMPLATE =
      "SELECT j.job_id FROM gobblin_job_executions j, " + "(SELECT MAX(last_modified_ts) AS most_recent_ts, job_name "
          + "FROM gobblin_job_executions GROUP BY job_name) max_results "
          + "WHERE j.job_name = max_results.job_name AND j.last_modified_ts = max_results.most_recent_ts";
  private static final String LIST_RECENT_JOB_EXECUTION_QUERY_TEMPLATE = "SELECT job_id FROM gobblin_job_executions";

  private static final String JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE =
      "SELECT j.job_name FROM gobblin_job_executions j, gobblin_task_executions t "
          + "WHERE j.job_id=t.job_id AND %s GROUP BY j.job_name";

  private static final String JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE =
      "SELECT job_id FROM gobblin_job_executions WHERE job_name=?";

  private static final String JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_job_executions WHERE job_id=?";

  private static final String TASK_EXECUTION_EXIST_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_executions WHERE task_id=?";

  private static final String TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_executions WHERE job_id=?";

  private static final String JOB_METRIC_EXIST_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_job_metrics " + "WHERE job_id=? AND metric_group=? AND metric_name=? AND metric_type=?";

  private static final String TASK_METRIC_EXIST_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_metrics " + "WHERE task_id=? AND metric_group=? AND metric_name=? AND metric_type=?";

  private static final String JOB_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT metric_group,metric_name,metric_type,metric_value FROM gobblin_job_metrics WHERE job_id=?";

  private static final String TASK_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT metric_group,metric_name,metric_type,metric_value FROM gobblin_task_metrics WHERE task_id=?";

  private static final String JOB_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_job_properties WHERE job_id=? AND property_key=?";

  private static final String TASK_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_properties WHERE task_id=? AND property_key=?";

  private static final String JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE =
      "SELECT property_key, property_value FROM gobblin_job_properties WHERE job_id=?";

  private static final String TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE =
      "SELECT property_key, property_value FROM gobblin_task_properties WHERE task_id=?";

  private static final Timestamp DEFAULT_TIMESTAMP = new Timestamp(1000L);

  private DataSource dataSource;

  @Override
  public void init(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public synchronized void put(JobExecutionInfo jobExecutionInfo) throws IOException {
    Optional<Connection> connectionOptional = Optional.absent();
    try {
      connectionOptional = Optional.of(getConnection());
      Connection connection = connectionOptional.get();
      connection.setAutoCommit(false);

      // Insert or update job execution information
      if (existsJobExecutionInfo(connection, jobExecutionInfo)) {
        updateJobExecutionInfo(connection, jobExecutionInfo);
      } else {
        insertJobExecutionInfo(connection, jobExecutionInfo);
      }

      // Insert or update job metrics
      if (jobExecutionInfo.hasMetrics()) {
        for (Metric metric : jobExecutionInfo.getMetrics()) {
          boolean insert =
              !existsMetric(connection, JOB_METRIC_EXIST_QUERY_STATEMENT_TEMPLATE, jobExecutionInfo.getJobId(), metric);
          updateMetric(connection, insert ? JOB_METRIC_INSERT_STATEMENT_TEMPLATE : JOB_METRIC_UPDATE_STATEMENT_TEMPLATE,
              jobExecutionInfo.getJobId(), metric, insert);
        }
      }

      // Insert or update job properties
      if (jobExecutionInfo.hasJobProperties()) {
        for (Map.Entry<String, String> entry : jobExecutionInfo.getJobProperties().entrySet()) {
          boolean insert = !existsProperty(connection, JOB_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE,
              jobExecutionInfo.getJobId(), entry.getKey());
          updateProperty(connection,
              insert ? JOB_PROPERTY_INSERT_STATEMENT_TEMPLATE : JOB_PROPERTY_UPDATE_STATEMENT_TEMPLATE,
              jobExecutionInfo.getJobId(), entry.getKey(), entry.getValue(), insert);
        }
      }

      // Insert or update task execution information
      if (jobExecutionInfo.hasTaskExecutions()) {
        for (TaskExecutionInfo info : jobExecutionInfo.getTaskExecutions()) {
          // Insert or update task execution information
          if (existsTaskExecutionInfo(connection, info)) {
            updateTaskExecutionInfo(connection, info);
          } else {
            insertTaskExecutionInfo(connection, info);
          }
          // Insert or update task metrics
          if (info.hasMetrics()) {
            for (Metric metric : info.getMetrics()) {
              boolean insert =
                  !existsMetric(connection, TASK_METRIC_EXIST_QUERY_STATEMENT_TEMPLATE, info.getTaskId(), metric);
              updateMetric(connection,
                  insert ? TASK_METRIC_INSERT_STATEMENT_TEMPLATE : TASK_METRIC_UPDATE_STATEMENT_TEMPLATE,
                  info.getTaskId(), metric, insert);
            }
          }

          // Insert or update task properties
          if (info.hasTaskProperties()) {
            for (Map.Entry<String, String> entry : info.getTaskProperties().entrySet()) {
              boolean insert = !existsProperty(connection, TASK_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE,
                  info.getTaskId(), entry.getKey());
              updateProperty(connection,
                  insert ? TASK_PROPERTY_INSERT_STATEMENT_TEMPLATE : TASK_PROPERTY_UPDATE_STATEMENT_TEMPLATE,
                  info.getTaskId(), entry.getKey(), entry.getValue(), insert);
            }
          }
        }
      }

      connection.commit();
    } catch (SQLException se) {
      LOGGER.error("Failed to put a new job execution information record", se);
      if (connectionOptional.isPresent()) {
        try {
          connectionOptional.get().rollback();
        } catch (SQLException se1) {
          LOGGER.error("Failed to rollback", se1);
        }
      }
      throw new IOException(se);
    } finally {
      if (connectionOptional.isPresent()) {
        try {
          connectionOptional.get().close();
        } catch (SQLException se) {
          LOGGER.error("Failed to close connection", se);
        }
      }
    }
  }

  @Override
  public synchronized List<JobExecutionInfo> get(JobExecutionQuery query) throws IOException {
    Preconditions.checkArgument(query.hasId() && query.hasIdType());

    Optional<Connection> connectionOptional = Optional.absent();
    try {
      connectionOptional = Optional.of(getConnection());
      Connection connection = connectionOptional.get();

      switch (query.getIdType()) {
        case JOB_ID:
          List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
          JobExecutionInfo jobExecutionInfo =
              processQueryById(connection, query.getId().getString(), query, Filter.MISSING);
          if (jobExecutionInfo != null) {
            jobExecutionInfos.add(jobExecutionInfo);
          }
          return jobExecutionInfos;
        case JOB_NAME:
          return processQueryByJobName(connection, query.getId().getString(), query, Filter.MISSING);
        case TABLE:
          return processQueryByTable(connection, query);
        case LIST_TYPE:
          return processListQuery(connection, query);
        default:
          throw new IOException("Unsupported query ID type: " + query.getIdType().name());
      }
    } catch (SQLException se) {
      LOGGER.error("Failed to execute query: " + query, se);
      throw new IOException(se);
    } finally {
      if (connectionOptional.isPresent()) {
        try {
          connectionOptional.get().close();
        } catch (SQLException se) {
          LOGGER.error("Failed to close connection", se);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }

  private Connection getConnection() throws SQLException {
    return this.dataSource.getConnection();
  }

  private static boolean existsJobExecutionInfo(Connection connection, JobExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasJobId());

    try (PreparedStatement queryStatement = connection
        .prepareStatement(JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE)) {
      queryStatement.setString(1, info.getJobId());
      try (ResultSet resultSet = queryStatement.executeQuery()) {
        return resultSet.next();
      }
    }
  }

  private static void insertJobExecutionInfo(Connection connection, JobExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasJobName());
    Preconditions.checkArgument(info.hasJobId());

    try (PreparedStatement insertStatement = connection.prepareStatement(JOB_EXECUTION_INSERT_STATEMENT_TEMPLATE)) {
      int index = 0;
      insertStatement.setString(++index, info.getJobName());
      insertStatement.setString(++index, info.getJobId());
      insertStatement
          .setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : DEFAULT_TIMESTAMP,
              getCalendarUTCInstance());
      insertStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : DEFAULT_TIMESTAMP,
          getCalendarUTCInstance());
      insertStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
      insertStatement.setString(++index, info.hasState() ? info.getState().name() : null);
      insertStatement.setInt(++index, info.hasLaunchedTasks() ? info.getLaunchedTasks() : -1);
      insertStatement.setInt(++index, info.hasCompletedTasks() ? info.getCompletedTasks() : -1);
      insertStatement.setString(++index, info.hasLauncherType() ? info.getLauncherType().name() : null);
      insertStatement.setString(++index, info.hasTrackingUrl() ? info.getTrackingUrl() : null);
      insertStatement.executeUpdate();
    }
  }

  private static void updateJobExecutionInfo(Connection connection, JobExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasJobId());

    try (PreparedStatement updateStatement = connection.prepareStatement(JOB_EXECUTION_UPDATE_STATEMENT_TEMPLATE)) {
      int index = 0;
      updateStatement
          .setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : DEFAULT_TIMESTAMP,
              getCalendarUTCInstance());
      updateStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : DEFAULT_TIMESTAMP,
          getCalendarUTCInstance());
      updateStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
      updateStatement.setString(++index, info.hasState() ? info.getState().name() : null);
      updateStatement.setInt(++index, info.hasLaunchedTasks() ? info.getLaunchedTasks() : -1);
      updateStatement.setInt(++index, info.hasCompletedTasks() ? info.getCompletedTasks() : -1);
      updateStatement.setString(++index, info.hasLauncherType() ? info.getLauncherType().name() : null);
      updateStatement.setString(++index, info.hasTrackingUrl() ? info.getTrackingUrl() : null);
      updateStatement.setString(++index, info.getJobId());
      updateStatement.executeUpdate();
    }
  }

  private static boolean existsTaskExecutionInfo(Connection connection, TaskExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());

    try (PreparedStatement queryStatement = connection.prepareStatement(TASK_EXECUTION_EXIST_QUERY_STATEMENT_TEMPLATE)) {
      queryStatement.setString(1, info.getTaskId());
      try (ResultSet resultSet = queryStatement.executeQuery()) {
        return resultSet.next();
      }
    }
  }

  private static void insertTaskExecutionInfo(Connection connection, TaskExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());
    Preconditions.checkArgument(info.hasJobId());

    try (PreparedStatement insertStatement = connection.prepareStatement(TASK_EXECUTION_INSERT_STATEMENT_TEMPLATE)) {
      int index = 0;
      insertStatement.setString(++index, info.getTaskId());
      insertStatement.setString(++index, info.getJobId());
      insertStatement
          .setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : DEFAULT_TIMESTAMP,
              getCalendarUTCInstance());
      insertStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : DEFAULT_TIMESTAMP,
          getCalendarUTCInstance());
      insertStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
      insertStatement.setString(++index, info.hasState() ? info.getState().name() : null);
      insertStatement.setString(++index, info.hasFailureException() ? info.getFailureException() : null);
      insertStatement.setLong(++index, info.hasLowWatermark() ? info.getLowWatermark() : -1);
      insertStatement.setLong(++index, info.hasHighWatermark() ? info.getHighWatermark() : -1);
      insertStatement.setString(++index,
          info.hasTable() && info.getTable().hasNamespace() ? info.getTable().getNamespace() : null);
      insertStatement
          .setString(++index, info.hasTable() && info.getTable().hasName() ? info.getTable().getName() : null);
      insertStatement
          .setString(++index, info.hasTable() && info.getTable().hasType() ? info.getTable().getType().name() : null);
      insertStatement.executeUpdate();
    }
  }

  private static void updateTaskExecutionInfo(Connection connection, TaskExecutionInfo info) throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());

    try (PreparedStatement updateStatement = connection.prepareStatement(TASK_EXECUTION_UPDATE_STATEMENT_TEMPLATE)) {
      int index = 0;
      updateStatement
          .setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : DEFAULT_TIMESTAMP,
              getCalendarUTCInstance());
      updateStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : DEFAULT_TIMESTAMP,
          getCalendarUTCInstance());
      updateStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
      updateStatement.setString(++index, info.hasState() ? info.getState().name() : null);
      updateStatement.setString(++index, info.hasFailureException() ? info.getFailureException() : null);
      updateStatement.setLong(++index, info.hasLowWatermark() ? info.getLowWatermark() : -1);
      updateStatement.setLong(++index, info.hasHighWatermark() ? info.getHighWatermark() : -1);
      updateStatement.setString(++index,
          info.hasTable() && info.getTable().hasNamespace() ? info.getTable().getNamespace() : null);
      updateStatement
          .setString(++index, info.hasTable() && info.getTable().hasName() ? info.getTable().getName() : null);
      updateStatement
          .setString(++index, info.hasTable() && info.getTable().hasType() ? info.getTable().getType().name() : null);
      updateStatement.setString(++index, info.getTaskId());
      updateStatement.executeUpdate();
    }
  }

  private static boolean existsMetric(Connection connection, String template, String id, Metric metric)
      throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(metric.hasGroup());
    Preconditions.checkArgument(metric.hasName());
    Preconditions.checkArgument(metric.hasType());

    try (PreparedStatement queryStatement = connection.prepareStatement(template)) {
      int index = 0;
      queryStatement.setString(++index, id);
      queryStatement.setString(++index, metric.getGroup());
      queryStatement.setString(++index, metric.getName());
      queryStatement.setString(++index, metric.getType().name());
      try (ResultSet resultSet = queryStatement.executeQuery()) {
        return resultSet.next();
      }
    }
  }

  private static void updateMetric(Connection connection, String template, String id, Metric metric, boolean insert)
      throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(metric.hasGroup());
    Preconditions.checkArgument(metric.hasName());
    Preconditions.checkArgument(metric.hasType());
    Preconditions.checkArgument(metric.hasValue());

    try (PreparedStatement updateStatement = connection.prepareStatement(template)) {
      int index = 0;
      if (insert) {
        updateStatement.setString(++index, id);
        updateStatement.setString(++index, metric.getGroup());
        updateStatement.setString(++index, metric.getName());
        updateStatement.setString(++index, metric.getType().name());
        updateStatement.setString(++index, metric.getValue());
      } else {
        updateStatement.setString(++index, metric.getValue());
        updateStatement.setString(++index, id);
        updateStatement.setString(++index, metric.getGroup());
        updateStatement.setString(++index, metric.getName());
        updateStatement.setString(++index, metric.getType().name());
      }
      updateStatement.executeUpdate();
    }
  }

  private static boolean existsProperty(Connection connection, String template, String id, String key)
      throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));

    try (PreparedStatement queryStatement = connection.prepareStatement(template)) {
      int index = 0;
      queryStatement.setString(++index, id);
      queryStatement.setString(++index, key);
      try (ResultSet resultSet = queryStatement.executeQuery()) {
        return resultSet.next();
      }
    }
  }

  private static void updateProperty(Connection connection, String template, String id, String key, String value,
                     boolean insert) throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

    try (PreparedStatement updateStatement = connection.prepareStatement(template)) {
      int index = 0;
      if (insert) {
        updateStatement.setString(++index, id);
        updateStatement.setString(++index, key);
        updateStatement.setString(++index, value);
      } else {
        updateStatement.setString(++index, value);
        updateStatement.setString(++index, id);
        updateStatement.setString(++index, key);
      }
      updateStatement.executeUpdate();
    }
  }

  private JobExecutionInfo processQueryById(Connection connection, String jobId, JobExecutionQuery query,
                        Filter tableFilter) throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobId));

    // Query job execution information
    try (PreparedStatement jobExecutionQueryStatement = connection
        .prepareStatement(JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE)) {
      jobExecutionQueryStatement.setString(1, jobId);
      try (ResultSet jobRs = jobExecutionQueryStatement.executeQuery()) {
        if (!jobRs.next()) {
          return null;
        }

        JobExecutionInfo jobExecutionInfo = resultSetToJobExecutionInfo(jobRs);

        // Query job metrics
        if (query.isIncludeJobMetrics()) {
          try (PreparedStatement jobMetricQueryStatement = connection
              .prepareStatement(JOB_METRIC_QUERY_STATEMENT_TEMPLATE)) {
            jobMetricQueryStatement.setString(1, jobRs.getString(2));
            try (ResultSet jobMetricRs = jobMetricQueryStatement.executeQuery()) {
              MetricArray jobMetrics = new MetricArray();
              while (jobMetricRs.next()) {
                jobMetrics.add(resultSetToMetric(jobMetricRs));
              }
              // Add job metrics
              jobExecutionInfo.setMetrics(jobMetrics);
            }
          }
        }

        // Query job properties
        Set<String> requestedJobPropertyKeys = null;
        if (query.hasJobProperties()) {
          requestedJobPropertyKeys = new HashSet<>(Arrays.asList(query.getJobProperties().split(",")));
        }
        try (PreparedStatement jobPropertiesQueryStatement = connection
            .prepareStatement(JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE)) {
          jobPropertiesQueryStatement.setString(1, jobExecutionInfo.getJobId());
          try (ResultSet jobPropertiesRs = jobPropertiesQueryStatement.executeQuery()) {
            Map<String, String> jobProperties = Maps.newHashMap();
            while (jobPropertiesRs.next()) {
              Map.Entry<String, String> property = resultSetToProperty(jobPropertiesRs);
              if (requestedJobPropertyKeys == null || requestedJobPropertyKeys.contains(property.getKey())) {
                jobProperties.put(property.getKey(), property.getValue());
              }
            }
            // Add job properties
            jobExecutionInfo.setJobProperties(new StringMap(jobProperties));
          }
        }

        // Query task execution information
        if (query.isIncludeTaskExecutions()) {
          TaskExecutionInfoArray taskExecutionInfos = new TaskExecutionInfoArray();
          String taskExecutionQuery = TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE;
          // Add table filter if applicable
          if (tableFilter.isPresent()) {
            taskExecutionQuery += " AND " + tableFilter;
          }
          try (PreparedStatement taskExecutionQueryStatement = connection.prepareStatement(taskExecutionQuery)) {
            taskExecutionQueryStatement.setString(1, jobId);
            if (tableFilter.isPresent()) {
              tableFilter.addParameters(taskExecutionQueryStatement, 2);
            }
            try (ResultSet taskRs = taskExecutionQueryStatement.executeQuery()) {
              while (taskRs.next()) {
                TaskExecutionInfo taskExecutionInfo = resultSetToTaskExecutionInfo(taskRs);

                // Query task metrics for each task execution record
                if (query.isIncludeTaskMetrics()) {
                  try (PreparedStatement taskMetricQueryStatement = connection
                      .prepareStatement(TASK_METRIC_QUERY_STATEMENT_TEMPLATE)) {
                    taskMetricQueryStatement.setString(1, taskExecutionInfo.getTaskId());
                    try (ResultSet taskMetricRs = taskMetricQueryStatement.executeQuery()) {
                      MetricArray taskMetrics = new MetricArray();
                      while (taskMetricRs.next()) {
                        taskMetrics.add(resultSetToMetric(taskMetricRs));
                      }
                      // Add task metrics
                      taskExecutionInfo.setMetrics(taskMetrics);
                    }
                  }
                }

                taskExecutionInfos.add(taskExecutionInfo);

                // Query task properties
                Set<String> queryTaskPropertyKeys = null;
                if (query.hasTaskProperties()) {
                  queryTaskPropertyKeys = new HashSet<>(Arrays.asList(query.getTaskProperties().split(",")));
                }
                try (PreparedStatement taskPropertiesQueryStatement = connection
                    .prepareStatement(TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE)) {
                  taskPropertiesQueryStatement.setString(1, taskExecutionInfo.getTaskId());
                  try (ResultSet taskPropertiesRs = taskPropertiesQueryStatement.executeQuery()) {
                    Map<String, String> taskProperties = Maps.newHashMap();
                    while (taskPropertiesRs.next()) {
                      Map.Entry<String, String> property = resultSetToProperty(taskPropertiesRs);
                      if (queryTaskPropertyKeys == null || queryTaskPropertyKeys.contains(property.getKey())) {
                        taskProperties.put(property.getKey(), property.getValue());
                      }
                    }
                    // Add job properties
                    taskExecutionInfo.setTaskProperties(new StringMap(taskProperties));
                  }
                }
                // Add task properties
              }
              // Add task execution information
              jobExecutionInfo.setTaskExecutions(taskExecutionInfos);
            }
          }
        }
        return jobExecutionInfo;
      }
    }
  }

  private List<JobExecutionInfo> processQueryByJobName(Connection connection, String jobName, JobExecutionQuery query,
                             Filter tableFilter) throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName));

    // Construct the query for job IDs by a given job name
    Filter timeRangeFilter = Filter.MISSING;
    String jobIdByNameQuery = JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE;
    if (query.hasTimeRange()) {
      // Add time range filter if applicable
      try {
          timeRangeFilter = constructTimeRangeFilter(query.getTimeRange());
        if (timeRangeFilter.isPresent()) {
          jobIdByNameQuery += " AND " + timeRangeFilter;
        }
      } catch (ParseException pe) {
        LOGGER.error("Failed to parse the query time range", pe);
        throw new SQLException(pe);
      }
    }

    // Add ORDER BY
    jobIdByNameQuery += " ORDER BY created_ts DESC";

    List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
    // Query job IDs by the given job name
    try (PreparedStatement queryStatement = connection.prepareStatement(jobIdByNameQuery)) {
      int limit = query.getLimit();
      if (limit > 0) {
        queryStatement.setMaxRows(limit);
      }
      queryStatement.setString(1, jobName);
      if (timeRangeFilter.isPresent()) {
        timeRangeFilter.addParameters(queryStatement, 2);
      }
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          jobExecutionInfos.add(processQueryById(connection, rs.getString(1), query, tableFilter));
        }
      }
    }
    return jobExecutionInfos;
  }

  private List<JobExecutionInfo> processQueryByTable(Connection connection, JobExecutionQuery query)
      throws SQLException {
    Preconditions.checkArgument(query.getId().isTable());

    Filter tableFilter = constructTableFilter(query.getId().getTable());

    // Construct the query for job names by table definition
    String jobNameByTableQuery = String.format(JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE, tableFilter);

    List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
    // Query job names by table definition
    try (PreparedStatement queryStatement = connection.prepareStatement(jobNameByTableQuery)) {
      if (tableFilter.isPresent()) {
        tableFilter.addParameters(queryStatement, 1);
      }
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          jobExecutionInfos.addAll(processQueryByJobName(connection, rs.getString(1), query, tableFilter));
        }
      }
    }

    return jobExecutionInfos;
  }

  private List<JobExecutionInfo> processListQuery(Connection connection, JobExecutionQuery query) throws SQLException {
    Preconditions.checkArgument(query.getId().isQueryListType());

    Filter timeRangeFilter = Filter.MISSING;
    QueryListType queryType = query.getId().getQueryListType();
    String listJobExecutionsQuery = "";
    if (queryType == QueryListType.DISTINCT) {
      listJobExecutionsQuery = LIST_DISTINCT_JOB_EXECUTION_QUERY_TEMPLATE;
      if (query.hasTimeRange()) {
        try {
          timeRangeFilter = constructTimeRangeFilter(query.getTimeRange());
          if (timeRangeFilter.isPresent()) {
            listJobExecutionsQuery += " AND " + timeRangeFilter;
          }
        } catch (ParseException pe) {
          LOGGER.error("Failed to parse the query time range", pe);
          throw new SQLException(pe);
        }
      }
    } else {
      listJobExecutionsQuery = LIST_RECENT_JOB_EXECUTION_QUERY_TEMPLATE;
    }
    listJobExecutionsQuery += " ORDER BY last_modified_ts DESC";

    try (PreparedStatement queryStatement = connection.prepareStatement(listJobExecutionsQuery)) {
      int limit = query.getLimit();
      if (limit > 0) {
        queryStatement.setMaxRows(limit);
      }
      if (timeRangeFilter.isPresent()) {
        timeRangeFilter.addParameters(queryStatement, 1);
      }
      try (ResultSet rs = queryStatement.executeQuery()) {
        List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
        while (rs.next()) {
          jobExecutionInfos.add(processQueryById(connection, rs.getString(1), query, Filter.MISSING));
        }
        return jobExecutionInfos;
      }
    }
  }

  private JobExecutionInfo resultSetToJobExecutionInfo(ResultSet rs) throws SQLException {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();

    jobExecutionInfo.setJobName(rs.getString("job_name"));
    jobExecutionInfo.setJobId(rs.getString("job_id"));
    try {
      jobExecutionInfo.setStartTime(rs.getTimestamp("start_time").getTime());
    } catch (SQLException se) {
      jobExecutionInfo.setStartTime(0);
    }
    try {
      jobExecutionInfo.setEndTime(rs.getTimestamp("end_time").getTime());
    } catch (SQLException se) {
      jobExecutionInfo.setEndTime(0);
    }
    jobExecutionInfo.setDuration(rs.getLong("duration"));
    String state = rs.getString("state");
    if (!Strings.isNullOrEmpty(state)) {
      jobExecutionInfo.setState(JobStateEnum.valueOf(state));
    }
    jobExecutionInfo.setLaunchedTasks(rs.getInt("launched_tasks"));
    jobExecutionInfo.setCompletedTasks(rs.getInt("completed_tasks"));
    String launcherType = rs.getString("launcher_type");
    if (!Strings.isNullOrEmpty(launcherType)) {
      jobExecutionInfo.setLauncherType(LauncherTypeEnum.valueOf(launcherType));
    }
    String trackingUrl = rs.getString("tracking_url");
    if (!Strings.isNullOrEmpty(trackingUrl)) {
      jobExecutionInfo.setTrackingUrl(trackingUrl);
    }
    return jobExecutionInfo;
  }

  private static TaskExecutionInfo resultSetToTaskExecutionInfo(ResultSet rs) throws SQLException {
    TaskExecutionInfo taskExecutionInfo = new TaskExecutionInfo();

    taskExecutionInfo.setTaskId(rs.getString("task_id"));
    taskExecutionInfo.setJobId(rs.getString("job_id"));
    try {
      taskExecutionInfo.setStartTime(rs.getTimestamp("start_time").getTime());
    } catch (SQLException se) {
      taskExecutionInfo.setStartTime(0);
    }
    try {
      taskExecutionInfo.setEndTime(rs.getTimestamp("end_time").getTime());
    } catch (SQLException se) {
      taskExecutionInfo.setEndTime(0);
    }
    taskExecutionInfo.setDuration(rs.getLong("duration"));
    String state = rs.getString("state");
    if (!Strings.isNullOrEmpty(state)) {
      taskExecutionInfo.setState(TaskStateEnum.valueOf(state));
    }
    String failureException = rs.getString("failure_exception");
    if (!Strings.isNullOrEmpty(failureException)) {
      taskExecutionInfo.setFailureException(failureException);
    }
    taskExecutionInfo.setLowWatermark(rs.getLong("low_watermark"));
    taskExecutionInfo.setHighWatermark(rs.getLong("high_watermark"));
    Table table = new Table();
    String namespace = rs.getString("table_namespace");
    if (!Strings.isNullOrEmpty(namespace)) {
      table.setNamespace(namespace);
    }
    String name = rs.getString("table_name");
    if (!Strings.isNullOrEmpty(name)) {
      table.setName(name);
    }
    String type = rs.getString("table_type");
    if (!Strings.isNullOrEmpty(type)) {
      table.setType(TableTypeEnum.valueOf(type));
    }
    taskExecutionInfo.setTable(table);

    return taskExecutionInfo;
  }

  private static Metric resultSetToMetric(ResultSet rs) throws SQLException {
    Metric metric = new Metric();

    metric.setGroup(rs.getString("metric_group"));
    metric.setName(rs.getString("metric_name"));
    metric.setType(MetricTypeEnum.valueOf(rs.getString("metric_type")));
    metric.setValue(rs.getString("metric_value"));

    return metric;
  }

  private static AbstractMap.SimpleEntry<String, String> resultSetToProperty(ResultSet rs) throws SQLException {
    return new AbstractMap.SimpleEntry<>(rs.getString(1), rs.getString(2));
  }

  private Filter constructTimeRangeFilter(TimeRange timeRange) throws ParseException {
    List<String> values = Lists.newArrayList();
    StringBuilder sb = new StringBuilder();

    if (!timeRange.hasTimeFormat()) {
      LOGGER.warn("Skipping the time range filter as there is no time format in: " + timeRange);
      return Filter.MISSING;
    }

    DateFormat dateFormat = new SimpleDateFormat(timeRange.getTimeFormat());

    boolean hasStartTime = timeRange.hasStartTime();
    if (hasStartTime) {
      sb.append("start_time>?");
      values.add(new Timestamp(dateFormat.parse(timeRange.getStartTime()).getTime()).toString());
    }

    if (timeRange.hasEndTime()) {
      if (hasStartTime) {
        sb.append(" AND ");
      }
      sb.append("end_time<?");
      values.add(new Timestamp(dateFormat.parse(timeRange.getEndTime()).getTime()).toString());
    }

    if (sb.length() > 0) {
      return new Filter(sb.toString(), values);
    }
    return Filter.MISSING;
  }

  private Filter constructTableFilter(Table table) {
    List<String> values = Lists.newArrayList();
    StringBuilder sb = new StringBuilder();

    boolean hasNamespace = table.hasNamespace();
    if (hasNamespace) {
      sb.append("table_namespace=?");
      values.add(table.getNamespace());
    }

    boolean hasName = table.hasName();
    if (hasName) {
      if (hasNamespace) {
        sb.append(" AND ");
      }
      sb.append("table_name=?");
      values.add(table.getName());
    }

    if (table.hasType()) {
      if (hasName) {
        sb.append(" AND ");
      }
      sb.append("table_type=?");
      values.add(table.getType().name());
    }

    if (sb.length() > 0) {
      return new Filter(sb.toString(), values);
    }
    return Filter.MISSING;
  }

  private static Calendar getCalendarUTCInstance() {
    return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  }
}
