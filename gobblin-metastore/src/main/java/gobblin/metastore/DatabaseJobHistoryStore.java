/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

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
import com.google.inject.Inject;

import com.linkedin.data.template.StringMap;

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
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author Yinan Li
 */
public class DatabaseJobHistoryStore implements JobHistoryStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseJobHistoryStore.class);

  private static final String JOB_EXECUTION_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_executions (job_name,job_id,start_time,end_time,duration,state,"
          + "launched_tasks,completed_tasks,launcher_type,tracking_url) VALUES(?,?,?,?,?,?,?,?,?,?)"
          + " ON DUPLICATE KEY UPDATE start_time=VALUES(start_time),end_time=VALUES(end_time),"
          + "duration=VALUES(duration),state=VALUES(state),launched_tasks=VALUES(launched_tasks),"
          + "completed_tasks=VALUES(completed_tasks),launcher_type=VALUES(launcher_type),"
          + "tracking_url=VALUES(tracking_url)";

  private static final String TASK_EXECUTION_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_executions (task_id,job_id,start_time,end_time,duration,"
          + "state,failure_exception,low_watermark,high_watermark,table_namespace,table_name,table_type) "
          + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE start_time=VALUES(start_time),"
          + "end_time=VALUES(end_time),duration=VALUES(duration),state=VALUES(state),"
          + "failure_exception=VALUES(failure_exception),low_watermark=VALUES(low_watermark),"
          + "high_watermark=VALUES(high_watermark),table_namespace=VALUES(table_namespace),"
          + "table_name=VALUES(table_name),table_type=VALUES(table_type)";

  private static final String JOB_METRIC_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_metrics (job_id,metric_group,metric_name,"
          + "metric_type,metric_value) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE "
          + "metric_value=VALUES(metric_value)";

  private static final String TASK_METRIC_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_metrics (task_id,metric_group,metric_name,"
          + "metric_type,metric_value) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE "
          + "metric_value=VALUES(metric_value)";

  private static final String JOB_PROPERTY_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_job_properties (job_id,property_key,property_value) VALUES(?,?,?)"
          + " ON DUPLICATE KEY UPDATE property_value=VALUES(property_value)";

  private static final String TASK_PROPERTY_UPSERT_STATEMENT_TEMPLATE =
      "INSERT INTO gobblin_task_properties (task_id,property_key,property_value) VALUES(?,?,?)"
          + " ON DUPLICATE KEY UPDATE property_value=VALUES(property_value)";

  private static final String LIST_DISTINCT_JOB_EXECUTION_QUERY_TEMPLATE =
      "SELECT j.job_id FROM gobblin_job_executions j, "
          + "(SELECT MAX(last_modified_ts) AS most_recent_ts, job_name "
          + "FROM gobblin_job_executions GROUP BY job_name) max_results "
          + "WHERE j.job_name = max_results.job_name AND j.last_modified_ts = max_results.most_recent_ts";
  private static final String LIST_RECENT_JOB_EXECUTION_QUERY_TEMPLATE =
      "SELECT job_id FROM gobblin_job_executions";

  private static final String JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE =
      "SELECT j.job_name FROM gobblin_job_executions j, gobblin_task_executions t "
          + "WHERE j.job_id=t.job_id AND %s GROUP BY j.job_name";

  private static final String JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE =
      "SELECT job_id FROM gobblin_job_executions WHERE job_name=?";

  private static final String JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_job_executions WHERE job_id=?";

  private static final String TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_executions WHERE job_id=?";

  private static final String JOB_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT metric_group,metric_name,metric_type,metric_value FROM gobblin_job_metrics WHERE job_id=?";

  private static final String TASK_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT metric_group,metric_name,metric_type,metric_value FROM gobblin_task_metrics WHERE task_id=?";

  private static final String JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE =
      "SELECT property_key, property_value FROM gobblin_job_properties WHERE job_id=?";

  private static final String TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE =
      "SELECT property_key, property_value FROM gobblin_task_properties WHERE task_id=?";

  private static final int DEFAULT_VALIDATION_TIMEOUT = 5;

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
      upsertJobExecutionInfo(connection, jobExecutionInfo);
      upsertJobMetrics(connection, jobExecutionInfo);
      upsertJobProperties(connection, jobExecutionInfo);

      // Insert or update task execution information
      if (jobExecutionInfo.hasTaskExecutions()) {
        upsertTaskExecutionInfos(connection, jobExecutionInfo.getTaskExecutions());
        upsertTaskMetrics(connection, jobExecutionInfo.getTaskExecutions());
        StringMap jobProperties = null;
        if (jobExecutionInfo.hasJobProperties()) {
          jobProperties = jobExecutionInfo.getJobProperties();
        }
        upsertTaskProperties(connection, jobProperties, jobExecutionInfo.getTaskExecutions());
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
              processQueryById(connection, query.getId().getString(), query, Optional.<String>absent());
          if (jobExecutionInfo != null) {
            jobExecutionInfos.add(jobExecutionInfo);
          }
          return jobExecutionInfos;
        case JOB_NAME:
          return processQueryByJobName(connection, query.getId().getString(), query, Optional.<String>absent());
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
  public void close()
      throws IOException {
    // Nothing to do
  }

  private Connection getConnection()
          throws SQLException {
    return this.dataSource.getConnection();
  }

  private void upsertJobExecutionInfo(Connection connection, JobExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasJobName());
    Preconditions.checkArgument(info.hasJobId());

    try (PreparedStatement upsertStatement = connection.prepareStatement(JOB_EXECUTION_UPSERT_STATEMENT_TEMPLATE)) {
      int index = 0;
      upsertStatement.setString(++index, info.getJobName());
      upsertStatement.setString(++index, info.getJobId());
      upsertStatement.setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : null,
              getCalendarUTCInstance());
      upsertStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : null,
              getCalendarUTCInstance());
      upsertStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
      upsertStatement.setString(++index, info.hasState() ? info.getState().name() : null);
      upsertStatement.setInt(++index, info.hasLaunchedTasks() ? info.getLaunchedTasks() : -1);
      upsertStatement.setInt(++index, info.hasCompletedTasks() ? info.getCompletedTasks() : -1);
      upsertStatement.setString(++index, info.hasLauncherType() ? info.getLauncherType().name() : null);
      upsertStatement.setString(++index, info.hasTrackingUrl() ? info.getTrackingUrl() : null);
      upsertStatement.executeUpdate();
    }
  }

  private void upsertTaskExecutionInfos(Connection connection, TaskExecutionInfoArray taskExecutions)
          throws SQLException {
    PreparedStatement upsertStatement = null;
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (upsertStatement == null) {
        upsertStatement = connection.prepareStatement(TASK_EXECUTION_UPSERT_STATEMENT_TEMPLATE);
      }
      upsertTaskExecutionInfo(upsertStatement, taskExecution);
      if (batchSize++ > 1000) {
        executeBatches(upsertStatement);
        upsertStatement = null;
      }
    }
    executeBatches(upsertStatement);
  }


  private void upsertJobProperties(Connection connection, JobExecutionInfo jobExecutionInfo) throws SQLException {
    if (jobExecutionInfo.hasJobProperties()) {
      PreparedStatement upsertStatement = null;
      int batchSize = 0;
      for (Map.Entry<String, String> property : jobExecutionInfo.getJobProperties().entrySet()) {
        if (upsertStatement == null) {
          upsertStatement = connection.prepareStatement(JOB_PROPERTY_UPSERT_STATEMENT_TEMPLATE);
        }
        upsertProperty(upsertStatement, property.getKey(), property.getValue(), jobExecutionInfo.getJobId());
        if (batchSize++ > 1000) {
          executeBatches(upsertStatement);
          upsertStatement = null;
        }
      }
      executeBatches(upsertStatement);
    }
  }

  private void upsertTaskProperties(Connection connection, StringMap jobProperties,
                                    TaskExecutionInfoArray taskExecutions)
        throws SQLException {
    PreparedStatement upsertStatement = null;
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (taskExecution.hasTaskProperties()) {
        for (Map.Entry<String, String> property : taskExecution.getTaskProperties().entrySet()) {
          if (jobProperties == null || !jobProperties.containsKey(property.getKey()) ||
                  !jobProperties.get(property.getKey()).equals(property.getValue())) {
            if (upsertStatement == null) {
              upsertStatement = connection.prepareStatement(TASK_PROPERTY_UPSERT_STATEMENT_TEMPLATE);
            }
            upsertProperty(upsertStatement, property.getKey(), property.getValue(), taskExecution.getTaskId());
            if (batchSize++ > 1000) {
              executeBatches(upsertStatement);
              upsertStatement = null;
            }
          }
        }
      }
    }
    executeBatches(upsertStatement);
  }

  private void upsertProperty(PreparedStatement upsertStatement, String key, String value, String id)
        throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

    int index = 0;
    upsertStatement.setString(++index, id);
    upsertStatement.setString(++index, key);
    upsertStatement.setString(++index, value);
    upsertStatement.addBatch();
  }

  private void upsertJobMetrics(Connection connection, JobExecutionInfo jobExecutionInfo) throws SQLException {
    if (jobExecutionInfo.hasMetrics()) {
      PreparedStatement upsertStatement = null;
      int batchSize = 0;
      for (Metric metric : jobExecutionInfo.getMetrics()) {
        if (upsertStatement == null) {
          upsertStatement = connection.prepareStatement(JOB_METRIC_UPSERT_STATEMENT_TEMPLATE);
        }
        upsertMetric(upsertStatement, metric, jobExecutionInfo.getJobId());
        if (batchSize++ > 1000) {
          executeBatches(upsertStatement);
          upsertStatement = null;
        }
      }
      executeBatches(upsertStatement);
    }
  }

  private void upsertTaskMetrics(Connection connection, TaskExecutionInfoArray taskExecutions)
        throws SQLException {
    PreparedStatement upsertStatement = null;
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (taskExecution.hasMetrics()) {
        for (Metric metric : taskExecution.getMetrics()) {
          if (upsertStatement == null) {
            upsertStatement = connection.prepareStatement(TASK_METRIC_UPSERT_STATEMENT_TEMPLATE);
          }
          upsertMetric(upsertStatement, metric, taskExecution.getTaskId());
          if (batchSize++ > 1000) {
            executeBatches(upsertStatement);
            upsertStatement = null;
          }
        }
      }
    }
    executeBatches(upsertStatement);
  }

  private void upsertMetric(PreparedStatement upsertStatement, Metric metric, String id) throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
    Preconditions.checkArgument(metric.hasGroup());
    Preconditions.checkArgument(metric.hasName());
    Preconditions.checkArgument(metric.hasType());
    Preconditions.checkArgument(metric.hasValue());

    int index = 0;
    upsertStatement.setString(++index, id);
    upsertStatement.setString(++index, metric.getGroup());
    upsertStatement.setString(++index, metric.getName());
    upsertStatement.setString(++index, metric.getType().name());
    upsertStatement.setString(++index, metric.getValue());
    upsertStatement.addBatch();
  }

  private void executeBatches(PreparedStatement upsertStatement) throws SQLException {
    if (upsertStatement != null) {
      try {
        upsertStatement.executeBatch();
      } finally {
        upsertStatement.close();
      }
    }
  }

  private void upsertTaskExecutionInfo(PreparedStatement upsertStatement, TaskExecutionInfo info)
      throws SQLException {
    Preconditions.checkArgument(info.hasTaskId());
    Preconditions.checkArgument(info.hasJobId());

    int index = 0;
    upsertStatement.setString(++index, info.getTaskId());
    upsertStatement.setString(++index, info.getJobId());
    upsertStatement.setTimestamp(++index, info.hasStartTime() ? new Timestamp(info.getStartTime()) : null,
            getCalendarUTCInstance());
    upsertStatement.setTimestamp(++index, info.hasEndTime() ? new Timestamp(info.getEndTime()) : null,
            getCalendarUTCInstance());
    upsertStatement.setLong(++index, info.hasDuration() ? info.getDuration() : -1);
    upsertStatement.setString(++index, info.hasState() ? info.getState().name() : null);
    upsertStatement.setString(++index, info.hasFailureException() ? info.getFailureException() : null);
    upsertStatement.setLong(++index, info.hasLowWatermark() ? info.getLowWatermark() : -1);
    upsertStatement.setLong(++index, info.hasHighWatermark() ? info.getHighWatermark() : -1);
    upsertStatement.setString(++index,
        info.hasTable() && info.getTable().hasNamespace() ? info.getTable().getNamespace() : null);
    upsertStatement.setString(++index, info.hasTable() && info.getTable().hasName() ? info.getTable().getName() : null);
    upsertStatement.setString(++index,
        info.hasTable() && info.getTable().hasType() ? info.getTable().getType().name() : null);
    upsertStatement.addBatch();
  }

  private JobExecutionInfo processQueryById(Connection connection, String jobId, JobExecutionQuery query,
      Optional<String> tableFilter) throws SQLException {
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
    if (query.isIncludeJobMetrics()) {
      PreparedStatement jobMetricQueryStatement = connection.prepareStatement(JOB_METRIC_QUERY_STATEMENT_TEMPLATE);
      jobMetricQueryStatement.setString(1, jobRs.getString(2));
      ResultSet jobMetricRs = jobMetricQueryStatement.executeQuery();
      MetricArray jobMetrics = new MetricArray();
      while (jobMetricRs.next()) {
        jobMetrics.add(resultSetToMetric(jobMetricRs));
      }
      // Add job metrics
      jobExecutionInfo.setMetrics(jobMetrics);
    }

    // Query job properties
    Set<String> requestedJobPropertyKeys = null;
    if (query.hasJobProperties()) {
      requestedJobPropertyKeys = new HashSet<String>(Arrays.asList(query.getJobProperties().split(",")));
    }
    PreparedStatement jobPropertiesQueryStatement = connection.prepareStatement(JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE);
    jobPropertiesQueryStatement.setString(1, jobExecutionInfo.getJobId());
    ResultSet jobPropertiesRs = jobPropertiesQueryStatement.executeQuery();
    Map<String, String> jobProperties = Maps.newHashMap();
    while (jobPropertiesRs.next()) {
      Map.Entry<String, String> property = resultSetToProperty(jobPropertiesRs);
      if (requestedJobPropertyKeys == null || requestedJobPropertyKeys.contains(property.getKey())) {
        jobProperties.put(property.getKey(), property.getValue());
      }
    }
    // Add job properties
    jobExecutionInfo.setJobProperties(new StringMap(jobProperties));

    // Query task execution information
    if (query.isIncludeTaskExecutions()) {
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
        if (query.isIncludeTaskMetrics()) {
          PreparedStatement taskMetricQueryStatement = connection.prepareStatement(TASK_METRIC_QUERY_STATEMENT_TEMPLATE);
          taskMetricQueryStatement.setString(1, taskExecutionInfo.getTaskId());
          ResultSet taskMetricRs = taskMetricQueryStatement.executeQuery();
          MetricArray taskMetrics = new MetricArray();
          while (taskMetricRs.next()) {
            taskMetrics.add(resultSetToMetric(taskMetricRs));
          }
          // Add task metrics
          taskExecutionInfo.setMetrics(taskMetrics);
        }

        taskExecutionInfos.add(taskExecutionInfo);

        // Query task properties
        Set<String> queryTaskPropertyKeys = null;
        if (query.hasTaskProperties()) {
          queryTaskPropertyKeys = new HashSet<String>(Arrays.asList(query.getTaskProperties().split(",")));
        }
        PreparedStatement taskPropertiesQueryStatement =
            connection.prepareStatement(TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE);
        taskPropertiesQueryStatement.setString(1, taskExecutionInfo.getTaskId());
        ResultSet taskPropertiesRs = taskPropertiesQueryStatement.executeQuery();
        Map<String, String> taskProperties = Maps.newHashMap();
        while (taskPropertiesRs.next()) {
          Map.Entry<String, String> property = resultSetToProperty(taskPropertiesRs);
          if (queryTaskPropertyKeys == null || queryTaskPropertyKeys.contains(property.getKey())) {
            taskProperties.put(property.getKey(), property.getValue());
          }
        }
        // Add job properties
        taskExecutionInfo.setTaskProperties(new StringMap(taskProperties));
        // Add task properties
      }
      // Add task execution information
      jobExecutionInfo.setTaskExecutions(taskExecutionInfos);
    }

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
    if (limit > 0) {
      queryStatement.setMaxRows(limit);
    }
    queryStatement.setString(1, jobName);
    ResultSet rs = queryStatement.executeQuery();
    while (rs.next()) {
      jobExecutionInfos.add(processQueryById(connection, rs.getString(1), query, tableFilter));
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

  private List<JobExecutionInfo> processListQuery(Connection connection, JobExecutionQuery query)
      throws SQLException {
    Preconditions.checkArgument(query.getId().isQueryListType());

    QueryListType queryType = query.getId().getQueryListType();
    String listJobExecutionsQuery = "";
    if (queryType == QueryListType.DISTINCT) {
      listJobExecutionsQuery = LIST_DISTINCT_JOB_EXECUTION_QUERY_TEMPLATE;
      if (query.hasTimeRange()) {
        try {
          String timeRangeFilter = constructTimeRangeFilter(query.getTimeRange());
          if (!Strings.isNullOrEmpty(timeRangeFilter)) {
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

    PreparedStatement queryStatement = connection.prepareStatement(listJobExecutionsQuery);
    int limit = query.getLimit();
    if (limit > 0) {
      queryStatement.setMaxRows(limit);
    }

    ResultSet rs = queryStatement.executeQuery();
    List<JobExecutionInfo> jobExecutionInfos = Lists.newArrayList();
    while (rs.next()) {
      jobExecutionInfos.add(processQueryById(connection, rs.getString(1), query, Optional.<String>absent()));
    }

    return jobExecutionInfos;
  }

  private JobExecutionInfo resultSetToJobExecutionInfo(ResultSet rs)
      throws SQLException {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();

    jobExecutionInfo.setJobName(rs.getString("job_name"));
    jobExecutionInfo.setJobId(rs.getString("job_id"));
    try {
      Timestamp startTime = rs.getTimestamp("start_time");
      if (startTime != null) {
        jobExecutionInfo.setStartTime(startTime.getTime());
      }
    } catch (SQLException se) {
      jobExecutionInfo.setStartTime(0);
    }
    try {
      Timestamp endTime = rs.getTimestamp("end_time");
      if (endTime != null) {
        jobExecutionInfo.setEndTime(endTime.getTime());
      }
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

  private TaskExecutionInfo resultSetToTaskExecutionInfo(ResultSet rs)
      throws SQLException {
    TaskExecutionInfo taskExecutionInfo = new TaskExecutionInfo();

    taskExecutionInfo.setTaskId(rs.getString("task_id"));
    taskExecutionInfo.setJobId(rs.getString("job_id"));
    try {
      Timestamp startTime = rs.getTimestamp("start_time");
      if (startTime != null) {
        taskExecutionInfo.setStartTime(startTime.getTime());
      }
    } catch (SQLException se) {
      taskExecutionInfo.setStartTime(0);
    }
    try {
      Timestamp endTime = rs.getTimestamp("end_time");
      if (endTime != null) {
          taskExecutionInfo.setEndTime(endTime.getTime());
      }
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

  private Metric resultSetToMetric(ResultSet rs)
      throws SQLException {
    Metric metric = new Metric();

    metric.setGroup(rs.getString("metric_group"));
    metric.setName(rs.getString("metric_name"));
    metric.setType(MetricTypeEnum.valueOf(rs.getString("metric_type")));
    metric.setValue(rs.getString("metric_value"));

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

  private static Calendar getCalendarUTCInstance() {
    return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  }
}
