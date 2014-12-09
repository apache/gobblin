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

package com.linkedin.uif.metastore;

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
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import com.linkedin.data.template.StringMap;
import com.linkedin.gobblin.rest.JobExecutionInfo;
import com.linkedin.gobblin.rest.JobExecutionQuery;
import com.linkedin.gobblin.rest.JobStateEnum;
import com.linkedin.gobblin.rest.Metric;
import com.linkedin.gobblin.rest.MetricArray;
import com.linkedin.gobblin.rest.MetricTypeEnum;
import com.linkedin.gobblin.rest.Table;
import com.linkedin.gobblin.rest.TableTypeEnum;
import com.linkedin.gobblin.rest.TaskExecutionInfo;
import com.linkedin.gobblin.rest.TaskExecutionInfoArray;
import com.linkedin.gobblin.rest.TaskStateEnum;
import com.linkedin.gobblin.rest.TimeRange;


/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author ynli
 */
public class DatabaseJobHistoryStore implements JobHistoryStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseJobHistoryStore.class);

  private static final String JOB_EXECUTION_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_executions (job_name,job_id,start_time,end_time,duration,"
          + "state,launched_tasks,completed_tasks) VALUES(?,?,?,?,?,?,?,?)";

  private static final String TASK_EXECUTION_INSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_executions (task_id,job_id,start_time,end_time,duration," +
          "state,low_watermark,high_watermark,table_namespace,table_name,table_type) " +
          "VALUES(?,?,?,?,?,?,?,?,?,?,?)";

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
          + "state=?,launched_tasks=?,completed_tasks=? WHERE job_id=?";

  private static final String TASK_EXECUTION_UPDATE_STATEMENT_TEMPLATE =
      "UPDATE gobblin_task_executions SET start_time=?,end_time=?,duration=?,state=?,low_watermark=?,"
          + "high_watermark=?,table_namespace=?,table_name=?,table_type=? WHERE task_id=?";

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

  private static final String DEFAULT_TIMESTAMP = "1970-01-01 00:00:00";

  private final DataSource dataSource;

  @Inject
  public DatabaseJobHistoryStore(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public synchronized void put(JobExecutionInfo jobExecutionInfo)
      throws IOException {
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
          boolean insert =
              !existsProperty(connection, JOB_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE, jobExecutionInfo.getJobId(),
                  entry.getKey());
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
              boolean insert =
                  !existsProperty(connection, TASK_PROPERTY_EXIST_QUERY_STATEMENT_TEMPLATE, info.getTaskId(),
                      entry.getKey());
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
  public synchronized List<JobExecutionInfo> get(JobExecutionQuery query)
      throws IOException {
    Preconditions.checkArgument(query.hasId() && query.hasIdType());

    Optional<Connection> connectionOptional = Optional.absent();
    try {
      connectionOptional = Optional.of(getConnection());
      Connection connection = connectionOptional.get();

      switch (query.getIdType()) {
        case JOB_ID:
          List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
          JobExecutionInfo jobExecutionInfo =
              processQueryById(connection, query.getId().getString(), Optional.<String>absent());
          if (jobExecutionInfo != null) {
            jobExecutionInfos.add(jobExecutionInfo);
          }
          return jobExecutionInfos;
        case JOB_NAME:
          return processQueryByJobName(connection, query.getId().getString(), query, Optional.<String>absent());
        case TABLE:
          return processQueryByTable(connection, query);
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
  public void close()
      throws IOException {
    // Nothing to do
  }

  private Connection getConnection()
      throws SQLException {
    return this.dataSource.getConnection();
  }

  private boolean existsJobExecutionInfo(Connection connection, JobExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasJobId());

    PreparedStatement queryStatement = connection.prepareStatement(JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE);
    queryStatement.setString(1, info.getJobId());
    return queryStatement.executeQuery().next();
  }

  private void insertJobExecutionInfo(Connection connection, JobExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasJobName());
    Preconditions.checkArgument(info.hasJobId());

    PreparedStatement insertStatement = connection.prepareStatement(JOB_EXECUTION_INSERT_STATEMENT_TEMPLATE);
    insertStatement.setString(1, info.getJobName());
    insertStatement.setString(2, info.getJobId());
    insertStatement.setTimestamp(3,
        info.hasStartTime() ? new Timestamp(info.getStartTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    insertStatement
        .setTimestamp(4, info.hasEndTime() ? new Timestamp(info.getEndTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    insertStatement.setLong(5, info.hasDuration() ? info.getDuration() : -1);
    insertStatement.setString(6, info.hasState() ? info.getState().name() : null);
    insertStatement.setInt(7, info.hasLaunchedTasks() ? info.getLaunchedTasks() : -1);
    insertStatement.setInt(8, info.hasCompletedTasks() ? info.getCompletedTasks() : -1);
    insertStatement.executeUpdate();
  }

  private void updateJobExecutionInfo(Connection connection, JobExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasJobId());

    PreparedStatement updateStatement = connection.prepareStatement(JOB_EXECUTION_UPDATE_STATEMENT_TEMPLATE);
    updateStatement.setTimestamp(1,
        info.hasStartTime() ? new Timestamp(info.getStartTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    updateStatement
        .setTimestamp(2, info.hasEndTime() ? new Timestamp(info.getEndTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    updateStatement.setLong(3, info.hasDuration() ? info.getDuration() : -1);
    updateStatement.setString(4, info.hasState() ? info.getState().name() : null);
    updateStatement.setInt(5, info.hasLaunchedTasks() ? info.getLaunchedTasks() : -1);
    updateStatement.setInt(6, info.hasCompletedTasks() ? info.getCompletedTasks() : -1);
    updateStatement.setString(7, info.getJobId());
    updateStatement.executeUpdate();
  }

  private boolean existsTaskExecutionInfo(Connection connection, TaskExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());

    PreparedStatement queryStatement = connection.prepareStatement(TASK_EXECUTION_EXIST_QUERY_STATEMENT_TEMPLATE);
    queryStatement.setString(1, info.getTaskId());
    return queryStatement.executeQuery().next();
  }

  private void insertTaskExecutionInfo(Connection connection, TaskExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());
    Preconditions.checkArgument(info.hasJobId());

    PreparedStatement insertStatement = connection.prepareStatement(TASK_EXECUTION_INSERT_STATEMENT_TEMPLATE);
    insertStatement.setString(1, info.getTaskId());
    insertStatement.setString(2, info.getJobId());
    insertStatement.setTimestamp(3,
        info.hasStartTime() ? new Timestamp(info.getStartTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    insertStatement
        .setTimestamp(4, info.hasEndTime() ? new Timestamp(info.getEndTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    insertStatement.setLong(5, info.hasDuration() ? info.getDuration() : -1);
    insertStatement.setString(6, info.hasState() ? info.getState().name() : null);
    insertStatement.setLong(7, info.hasLowWatermark() ? info.getLowWatermark() : -1);
    insertStatement.setLong(8, info.hasHighWatermark() ? info.getHighWatermark() : -1);
    insertStatement
        .setString(9, info.hasTable() && info.getTable().hasNamespace() ? info.getTable().getNamespace() : null);
    insertStatement.setString(10, info.hasTable() && info.getTable().hasName() ? info.getTable().getName() : null);
    insertStatement
        .setString(11, info.hasTable() && info.getTable().hasType() ? info.getTable().getType().name() : null);
    insertStatement.executeUpdate();
  }

  private void updateTaskExecutionInfo(Connection connection, TaskExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());

    PreparedStatement updateStatement = connection.prepareStatement(TASK_EXECUTION_UPDATE_STATEMENT_TEMPLATE);
    updateStatement.setTimestamp(1,
        info.hasStartTime() ? new Timestamp(info.getStartTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    updateStatement
        .setTimestamp(2, info.hasEndTime() ? new Timestamp(info.getEndTime()) : Timestamp.valueOf(DEFAULT_TIMESTAMP));
    updateStatement.setLong(3, info.hasDuration() ? info.getDuration() : -1);
    updateStatement.setString(4, info.hasState() ? info.getState().name() : null);
    updateStatement.setLong(5, info.hasLowWatermark() ? info.getLowWatermark() : -1);
    updateStatement.setLong(6, info.hasHighWatermark() ? info.getHighWatermark() : -1);
    updateStatement
        .setString(7, info.hasTable() && info.getTable().hasNamespace() ? info.getTable().getNamespace() : null);
    updateStatement.setString(8, info.hasTable() && info.getTable().hasName() ? info.getTable().getName() : null);
    updateStatement
        .setString(9, info.hasTable() && info.getTable().hasType() ? info.getTable().getType().name() : null);
    updateStatement.setString(10, info.getTaskId());
    updateStatement.executeUpdate();
  }

  private boolean existsMetric(Connection connection, String template, String id, Metric metric)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(metric.hasGroup());
    Preconditions.checkArgument(metric.hasName());
    Preconditions.checkArgument(metric.hasType());

    PreparedStatement queryStatement = connection.prepareStatement(template);
    queryStatement.setString(1, id);
    queryStatement.setString(2, metric.getGroup());
    queryStatement.setString(3, metric.getName());
    queryStatement.setString(4, metric.getType().name());
    return queryStatement.executeQuery().next();
  }

  private void updateMetric(Connection connection, String template, String id, Metric metric, boolean insert)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(metric.hasGroup());
    Preconditions.checkArgument(metric.hasName());
    Preconditions.checkArgument(metric.hasType());
    Preconditions.checkArgument(metric.hasValue());

    PreparedStatement updateStatement = connection.prepareStatement(template);
    if (insert) {
      updateStatement.setString(1, id);
      updateStatement.setString(2, metric.getGroup());
      updateStatement.setString(3, metric.getName());
      updateStatement.setString(4, metric.getType().name());
      updateStatement.setString(5, metric.getValue());
    } else {
      updateStatement.setString(1, metric.getValue());
      updateStatement.setString(2, id);
      updateStatement.setString(3, metric.getGroup());
      updateStatement.setString(4, metric.getName());
      updateStatement.setString(5, metric.getType().name());
    }
    updateStatement.executeUpdate();
  }

  private boolean existsProperty(Connection connection, String template, String id, String key)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));

    PreparedStatement queryStatement = connection.prepareStatement(template);
    queryStatement.setString(1, id);
    queryStatement.setString(2, key);
    return queryStatement.executeQuery().next();
  }

  private void updateProperty(Connection connection, String template, String id, String key, String value,
      boolean insert)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

    PreparedStatement updateStatement = connection.prepareStatement(template);
    if (insert) {
      updateStatement.setString(1, id);
      updateStatement.setString(2, key);
      updateStatement.setString(3, value);
    } else {
      updateStatement.setString(1, value);
      updateStatement.setString(2, id);
      updateStatement.setString(3, key);
    }
    updateStatement.executeUpdate();
  }

  private JobExecutionInfo processQueryById(Connection connection, String jobId, Optional<String> tableFilter)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobId));

    // Query job execution information
    PreparedStatement jobExecutionQueryStatement =
        connection.prepareStatement(JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE);
    jobExecutionQueryStatement.setString(1, jobId);
    ResultSet jobRs = jobExecutionQueryStatement.executeQuery();
    if (!jobRs.next()) {
      return null;
    }

    JobExecutionInfo jobExecutionInfo = resultSetToJobExecutionInfo(jobRs);

    // Query job metrics
    PreparedStatement jobMetricQueryStatement = connection.prepareStatement(JOB_METRIC_QUERY_STATEMENT_TEMPLATE);
    jobMetricQueryStatement.setString(1, jobRs.getString(2));
    ResultSet jobMetricRs = jobMetricQueryStatement.executeQuery();
    MetricArray jobMetrics = new MetricArray();
    while (jobMetricRs.next()) {
      jobMetrics.add(resultSetToMetric(jobMetricRs));
    }
    // Add job metrics
    jobExecutionInfo.setMetrics(jobMetrics);

    // Query job properties
    PreparedStatement jobPropertiesQueryStatement = connection.prepareStatement(JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE);
    jobPropertiesQueryStatement.setString(1, jobExecutionInfo.getJobId());
    ResultSet jobPropertiesRs = jobPropertiesQueryStatement.executeQuery();
    Map<String, String> jobProperties = Maps.newHashMap();
    while (jobPropertiesRs.next()) {
      Map.Entry<String, String> property = resultSetToProperty(jobPropertiesRs);
      jobProperties.put(property.getKey(), property.getValue());
    }
    // Add job properties
    jobExecutionInfo.setJobProperties(new StringMap(jobProperties));

    // Query task execution information
    TaskExecutionInfoArray taskExecutionInfos = new TaskExecutionInfoArray();
    String taskExecutionQuery = TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE;
    // Add table filter if applicable
    if (tableFilter.isPresent() && !Strings.isNullOrEmpty(tableFilter.get())) {
      taskExecutionQuery += " AND " + tableFilter.get();
    }
    PreparedStatement taskExecutionQueryStatement = connection.prepareStatement(taskExecutionQuery);
    taskExecutionQueryStatement.setString(1, jobId);
    ResultSet taskRs = taskExecutionQueryStatement.executeQuery();
    while (taskRs.next()) {
      TaskExecutionInfo taskExecutionInfo = resultSetToTaskExecutionInfo(taskRs);
      // Query task metrics for each task execution record
      PreparedStatement taskMetricQueryStatement = connection.prepareStatement(TASK_METRIC_QUERY_STATEMENT_TEMPLATE);
      taskMetricQueryStatement.setString(1, taskExecutionInfo.getTaskId());
      ResultSet taskMetricRs = taskMetricQueryStatement.executeQuery();
      MetricArray taskMetrics = new MetricArray();
      while (taskMetricRs.next()) {
        taskMetrics.add(resultSetToMetric(taskMetricRs));
      }
      // Add task metrics
      taskExecutionInfo.setMetrics(taskMetrics);
      taskExecutionInfos.add(taskExecutionInfo);

      // Query task properties
      PreparedStatement taskPropertiesQueryStatement =
          connection.prepareStatement(TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE);
      taskPropertiesQueryStatement.setString(1, taskExecutionInfo.getTaskId());
      ResultSet taskPropertiesRs = taskPropertiesQueryStatement.executeQuery();
      Map<String, String> taskProperties = Maps.newHashMap();
      while (taskPropertiesRs.next()) {
        Map.Entry<String, String> property = resultSetToProperty(taskPropertiesRs);
        taskProperties.put(property.getKey(), property.getValue());
      }
      // Add job properties
      taskExecutionInfo.setTaskProperties(new StringMap(taskProperties));
      // Add task properties
    }
    // Add task execution information
    jobExecutionInfo.setTaskExecutions(taskExecutionInfos);

    return jobExecutionInfo;
  }

  private List<JobExecutionInfo> processQueryByJobName(Connection connection, String jobName, JobExecutionQuery query,
      Optional<String> tableFilter)
      throws SQLException {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName));

    // Construct the query for job IDs by a given job name
    String jobIdByNameQuery = JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE;
    if (query.hasTimeRange()) {
      // Add time range filter if applicable
      try {
        String timeRangeFilter = constructTimeRangeFilter(query.getTimeRange());
        if (!Strings.isNullOrEmpty(timeRangeFilter)) {
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
    PreparedStatement queryStatement = connection.prepareStatement(jobIdByNameQuery);
    int limit = query.getLimit();
    // Add LIMIT if applicable
    if (limit > 0) {
      queryStatement.setMaxRows(limit);
    }
    queryStatement.setString(1, jobName);
    ResultSet rs = queryStatement.executeQuery();
    while (rs.next()) {
      jobExecutionInfos.add(processQueryById(connection, rs.getString(1), tableFilter));
    }

    return jobExecutionInfos;
  }

  private List<JobExecutionInfo> processQueryByTable(Connection connection, JobExecutionQuery query)
      throws SQLException {

    Preconditions.checkArgument(query.getId().isTable());

    String tableFilter = constructTableFilter(query.getId().getTable());

    // Construct the query for job names by table definition
    String jobNameByTableQuery = String.format(JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE, tableFilter);

    List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
    // Query job names by table definition
    PreparedStatement queryStatement = connection.prepareStatement(jobNameByTableQuery);
    ResultSet rs = queryStatement.executeQuery();
    while (rs.next()) {
      jobExecutionInfos.addAll(processQueryByJobName(connection, rs.getString(1), query, Optional.of(tableFilter)));
    }

    return jobExecutionInfos;
  }

  private JobExecutionInfo resultSetToJobExecutionInfo(ResultSet rs)
      throws SQLException {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();

    jobExecutionInfo.setJobName(rs.getString(1));
    jobExecutionInfo.setJobId(rs.getString(2));
    try {
      jobExecutionInfo.setStartTime(rs.getTimestamp(3).getTime());
    } catch (SQLException se) {
      jobExecutionInfo.setStartTime(0);
    }
    try {
      jobExecutionInfo.setEndTime(rs.getTimestamp(4).getTime());
    } catch (SQLException se) {
      jobExecutionInfo.setEndTime(0);
    }
    jobExecutionInfo.setDuration(rs.getLong(5));
    String state = rs.getString(6);
    if (!Strings.isNullOrEmpty(state)) {
      jobExecutionInfo.setState(JobStateEnum.valueOf(state));
    }
    jobExecutionInfo.setLaunchedTasks(rs.getInt(7));
    jobExecutionInfo.setCompletedTasks(rs.getInt(8));

    return jobExecutionInfo;
  }

  private TaskExecutionInfo resultSetToTaskExecutionInfo(ResultSet rs)
      throws SQLException {
    TaskExecutionInfo taskExecutionInfo = new TaskExecutionInfo();

    taskExecutionInfo.setTaskId(rs.getString(1));
    taskExecutionInfo.setJobId(rs.getString(2));
    try {
      taskExecutionInfo.setStartTime(rs.getTimestamp(3).getTime());
    } catch (SQLException se) {
      taskExecutionInfo.setStartTime(0);
    }
    try {
      taskExecutionInfo.setEndTime(rs.getTimestamp(4).getTime());
    } catch (SQLException se) {
      taskExecutionInfo.setEndTime(0);
    }
    taskExecutionInfo.setDuration(rs.getLong(5));
    String state = rs.getString(6);
    if (!Strings.isNullOrEmpty(state)) {
      taskExecutionInfo.setState(TaskStateEnum.valueOf(state));
    }
    taskExecutionInfo.setLowWatermark(rs.getLong(7));
    taskExecutionInfo.setHighWatermark(rs.getLong(8));
    Table table = new Table();
    String namespace = rs.getString(9);
    if (!Strings.isNullOrEmpty(namespace)) {
      table.setNamespace(namespace);
    }
    String name = rs.getString(10);
    if (!Strings.isNullOrEmpty(name)) {
      table.setName(name);
    }
    String type = rs.getString(11);
    if (!Strings.isNullOrEmpty(type)) {
      table.setType(TableTypeEnum.valueOf(type));
    }
    taskExecutionInfo.setTable(table);

    return taskExecutionInfo;
  }

  private Metric resultSetToMetric(ResultSet rs)
      throws SQLException {
    Metric metric = new Metric();

    metric.setGroup(rs.getString(1));
    metric.setName(rs.getString(2));
    metric.setType(MetricTypeEnum.valueOf(rs.getString(3)));
    metric.setValue(rs.getString(4));

    return metric;
  }

  private AbstractMap.SimpleEntry<String, String> resultSetToProperty(ResultSet rs)
      throws SQLException {

    return new AbstractMap.SimpleEntry<String, String>(rs.getString(1), rs.getString(2));
  }

  private String constructTimeRangeFilter(TimeRange timeRange)
      throws ParseException {
    StringBuilder sb = new StringBuilder();

    if (!timeRange.hasTimeFormat()) {
      LOGGER.warn("Skipping the time range filter as there is no time format in: " + timeRange);
      return "";
    }

    DateFormat dateFormat = new SimpleDateFormat(timeRange.getTimeFormat());

    boolean hasStartTime = timeRange.hasStartTime();
    if (hasStartTime) {
      Timestamp startTimestamp = new Timestamp(dateFormat.parse(timeRange.getStartTime()).getTime());
      sb.append("start_time>'").append(startTimestamp.toString()).append("'");
    }

    if (timeRange.hasEndTime()) {
      if (hasStartTime) {
        sb.append(" AND ");
      }
      Timestamp endTimestamp = new Timestamp(dateFormat.parse(timeRange.getEndTime()).getTime());
      sb.append("end_time<'").append(endTimestamp.toString()).append("'");
    }

    return sb.toString();
  }

  private String constructTableFilter(Table table) {
    StringBuilder sb = new StringBuilder();

    boolean hasNamespace = table.hasNamespace();
    if (hasNamespace) {
      sb.append("table_namespace='").append(table.getNamespace()).append("'");
    }

    boolean hasName = table.hasName();
    if (hasName) {
      if (hasNamespace) {
        sb.append(" AND ");
      }
      sb.append("table_name='").append(table.getName()).append("'");
    }

    if (table.hasType()) {
      if (hasName) {
        sb.append(" AND ");
      }
      sb.append("table_type='").append(table.getType().name()).append("'");
    }

    return sb.toString();
  }
}
