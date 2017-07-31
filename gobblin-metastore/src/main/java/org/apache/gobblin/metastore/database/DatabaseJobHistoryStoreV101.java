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

package org.apache.gobblin.metastore.database;

import javax.sql.DataSource;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.StringMap;

import org.apache.gobblin.metastore.JobHistoryStore;
import org.apache.gobblin.rest.JobExecutionInfo;
import org.apache.gobblin.rest.JobExecutionQuery;
import org.apache.gobblin.rest.JobStateEnum;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.rest.Metric;
import org.apache.gobblin.rest.MetricArray;
import org.apache.gobblin.rest.MetricTypeEnum;
import org.apache.gobblin.rest.QueryListType;
import org.apache.gobblin.rest.Table;
import org.apache.gobblin.rest.TableTypeEnum;
import org.apache.gobblin.rest.TaskExecutionInfo;
import org.apache.gobblin.rest.TaskExecutionInfoArray;
import org.apache.gobblin.rest.TaskStateEnum;
import org.apache.gobblin.rest.TimeRange;


/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author Yinan Li
 */
@SupportedDatabaseVersion(isDefault = false, version = "1.0.1")
public class DatabaseJobHistoryStoreV101 implements VersionedDatabaseJobHistoryStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseJobHistoryStoreV101.class);

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
          + "FROM gobblin_job_executions%s GROUP BY job_name) max_results "
          + "WHERE j.job_name = max_results.job_name AND j.last_modified_ts = max_results.most_recent_ts";
  private static final String LIST_RECENT_JOB_EXECUTION_QUERY_TEMPLATE =
      "SELECT job_id FROM gobblin_job_executions%s";

  private static final String JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE =
      "SELECT j.job_name FROM gobblin_job_executions j, gobblin_task_executions t "
          + "WHERE j.job_id=t.job_id AND %s%s GROUP BY j.job_name";

  private static final String JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE =
      "SELECT job_id FROM gobblin_job_executions WHERE job_name=?";

  private static final String JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_job_executions WHERE job_id IN (%s)";

  private static final String TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE =
      "SELECT * FROM gobblin_task_executions WHERE job_id IN (%s)";

  private static final String JOB_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT job_id,metric_group,metric_name,metric_type,metric_value FROM gobblin_job_metrics WHERE job_id IN (%s)";

  private static final String TASK_METRIC_QUERY_STATEMENT_TEMPLATE =
      "SELECT job_id,m.task_id,metric_group,metric_name,metric_type,metric_value FROM gobblin_task_metrics m JOIN gobblin_task_executions t ON t.task_id = m.task_id WHERE job_id IN (%s)";

  private static final String JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE =
    "SELECT job_id,property_key,property_value FROM gobblin_job_properties WHERE job_id IN (%s)";

  private static final String TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE =
    "SELECT job_id,p.task_id,property_key,property_value FROM gobblin_task_properties p JOIN gobblin_task_executions t ON t.task_id = p.task_id WHERE job_id IN (%s)";

  private static final String FILTER_JOBS_WITH_TASKS = "(`state` != 'COMMITTED' OR launched_tasks > 0)";

  private DataSource dataSource;

  @Override
  public void init(DataSource dataSource) {
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
        Optional<StringMap> jobProperties = Optional.absent();
        if (jobExecutionInfo.hasJobProperties()) {
          jobProperties = Optional.of(jobExecutionInfo.getJobProperties());
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
          return processQueryByIds(connection, query, Filter.MISSING, Lists.newArrayList(query.getId().getString()));
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
  public void close()
      throws IOException {
    // Nothing to do
  }

  private Connection getConnection()
          throws SQLException {
    return this.dataSource.getConnection();
  }

  protected String getLauncherType(JobExecutionInfo info) {
    if (info.hasLauncherType()) {
      if (info.getLauncherType() == LauncherTypeEnum.CLUSTER) {
        return LauncherTypeEnum.YARN.name();
      }
      return info.getLauncherType().name();
    }
    return null;
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
      upsertStatement.setString(++index, getLauncherType(info));
      upsertStatement.setString(++index, info.hasTrackingUrl() ? info.getTrackingUrl() : null);
      upsertStatement.executeUpdate();
    }
  }

  private void upsertTaskExecutionInfos(Connection connection, TaskExecutionInfoArray taskExecutions)
          throws SQLException {
    Optional<PreparedStatement> upsertStatement = Optional.absent();
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (!upsertStatement.isPresent()) {
        upsertStatement = Optional.of(connection.prepareStatement(TASK_EXECUTION_UPSERT_STATEMENT_TEMPLATE));
      }
      addTaskExecutionInfoToBatch(upsertStatement.get(), taskExecution);
      if (batchSize++ > 1000) {
        executeBatches(upsertStatement);
        upsertStatement = Optional.absent();
        batchSize = 0;
      }
    }
    executeBatches(upsertStatement);
  }


  private void upsertJobProperties(Connection connection, JobExecutionInfo jobExecutionInfo) throws SQLException {
    if (jobExecutionInfo.hasJobProperties()) {
      Optional<PreparedStatement> upsertStatement = Optional.absent();
      int batchSize = 0;
      for (Map.Entry<String, String> property : jobExecutionInfo.getJobProperties().entrySet()) {
        if (!upsertStatement.isPresent()) {
          upsertStatement = Optional.of(connection.prepareStatement(JOB_PROPERTY_UPSERT_STATEMENT_TEMPLATE));
        }
        addPropertyToBatch(upsertStatement.get(), property.getKey(), property.getValue(), jobExecutionInfo.getJobId());
        if (batchSize++ > 1000) {
          executeBatches(upsertStatement);
          upsertStatement = Optional.absent();
          batchSize = 0;
        }
      }
      executeBatches(upsertStatement);
    }
  }

  private void upsertTaskProperties(Connection connection, Optional<StringMap> jobProperties,
                                    TaskExecutionInfoArray taskExecutions)
        throws SQLException {
    Optional<PreparedStatement> upsertStatement = Optional.absent();
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (taskExecution.hasTaskProperties()) {
        for (Map.Entry<String, String> property : taskExecution.getTaskProperties().entrySet()) {
          if (!jobProperties.isPresent() || !jobProperties.get().containsKey(property.getKey()) ||
                  !jobProperties.get().get(property.getKey()).equals(property.getValue())) {
            if (!upsertStatement.isPresent()) {
              upsertStatement = Optional.of(connection.prepareStatement(TASK_PROPERTY_UPSERT_STATEMENT_TEMPLATE));
            }
            addPropertyToBatch(upsertStatement.get(), property.getKey(), property.getValue(), taskExecution.getTaskId());
            if (batchSize++ > 1000) {
              executeBatches(upsertStatement);
              upsertStatement = Optional.absent();
              batchSize = 0;
            }
          }
        }
      }
    }
    executeBatches(upsertStatement);
  }

  private void addPropertyToBatch(PreparedStatement upsertStatement, String key, String value, String id)
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
      Optional<PreparedStatement> upsertStatement = Optional.absent();
      int batchSize = 0;
      for (Metric metric : jobExecutionInfo.getMetrics()) {
        if (!upsertStatement.isPresent()) {
          upsertStatement = Optional.of(connection.prepareStatement(JOB_METRIC_UPSERT_STATEMENT_TEMPLATE));
        }
        addMetricToBatch(upsertStatement.get(), metric, jobExecutionInfo.getJobId());
        if (batchSize++ > 1000) {
          executeBatches(upsertStatement);
          upsertStatement = Optional.absent();
          batchSize = 0;
        }
      }
      executeBatches(upsertStatement);
    }
  }

  private void upsertTaskMetrics(Connection connection, TaskExecutionInfoArray taskExecutions)
        throws SQLException {
    Optional<PreparedStatement> upsertStatement = Optional.absent();
    int batchSize = 0;
    for (TaskExecutionInfo taskExecution : taskExecutions) {
      if (taskExecution.hasMetrics()) {
        for (Metric metric : taskExecution.getMetrics()) {
          if (!upsertStatement.isPresent()) {
            upsertStatement = Optional.of(connection.prepareStatement(TASK_METRIC_UPSERT_STATEMENT_TEMPLATE));
          }
          addMetricToBatch(upsertStatement.get(), metric, taskExecution.getTaskId());
          if (batchSize++ > 1000) {
            executeBatches(upsertStatement);
            upsertStatement = Optional.absent();
            batchSize = 0;
          }
        }
      }
    }
    executeBatches(upsertStatement);
  }

  private void executeBatches(Optional<PreparedStatement> upsertStatement) throws SQLException {
    if (upsertStatement.isPresent()) {
      try {
        upsertStatement.get().executeBatch();
      } finally {
        upsertStatement.get().close();
      }
    }
  }

  private void addMetricToBatch(PreparedStatement upsertStatement, Metric metric, String id) throws SQLException {
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

  private void addTaskExecutionInfoToBatch(PreparedStatement upsertStatement, TaskExecutionInfo info)
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

  private List<JobExecutionInfo> processQueryByIds(Connection connection, JobExecutionQuery query,
                                                   Filter tableFilter, List<String> jobIds)
      throws SQLException {
    Map<String, JobExecutionInfo> jobExecutionInfos = getJobExecutionInfos(connection, jobIds);
    addMetricsToJobExecutions(connection, query, jobExecutionInfos);
    addPropertiesToJobExecutions(connection, query, jobExecutionInfos);
    addTasksToJobExecutions(connection, query, tableFilter, jobExecutionInfos);

    return ImmutableList.copyOf(jobExecutionInfos.values());
  }

  private Map<String, JobExecutionInfo> getJobExecutionInfos(Connection connection, List<String> jobIds)
      throws SQLException {
    Map<String, JobExecutionInfo> jobExecutionInfos = Maps.newLinkedHashMap();
    if (jobIds != null && jobIds.size() > 0) {
      String template = String.format(JOB_EXECUTION_QUERY_BY_JOB_ID_STATEMENT_TEMPLATE, getInPredicate(jobIds.size()));
      int index = 1;
      try (PreparedStatement jobExecutionQueryStatement = connection.prepareStatement(template)) {
        for (String jobId : jobIds) {
          jobExecutionQueryStatement.setString(index++, jobId);
        }
        try (ResultSet jobRs = jobExecutionQueryStatement.executeQuery()) {
          while (jobRs.next()) {
            JobExecutionInfo jobExecutionInfo = resultSetToJobExecutionInfo(jobRs);
            jobExecutionInfos.put(jobExecutionInfo.getJobId(), jobExecutionInfo);
          }
        }
      }
    }
    return jobExecutionInfos;
  }

  private void addMetricsToJobExecutions(Connection connection, JobExecutionQuery query,
                                         Map<String, JobExecutionInfo> jobExecutionInfos) throws SQLException {
    if (query.isIncludeJobMetrics() && jobExecutionInfos.size() > 0) {
      String template = String.format(JOB_METRIC_QUERY_STATEMENT_TEMPLATE, getInPredicate(jobExecutionInfos.size()));
      int index = 1;
      try (PreparedStatement jobMetricQueryStatement = connection.prepareStatement(template)) {
        for (String jobId : jobExecutionInfos.keySet()) {
          jobMetricQueryStatement.setString(index++, jobId);
        }
        try (ResultSet jobMetricRs = jobMetricQueryStatement.executeQuery()) {
          while (jobMetricRs.next()) {
            String jobId = jobMetricRs.getString("job_id");
            JobExecutionInfo jobExecutionInfo = jobExecutionInfos.get(jobId);
            MetricArray metricArray = jobExecutionInfo.getMetrics(GetMode.NULL);
            if (metricArray == null) {
              metricArray = new MetricArray();
              jobExecutionInfo.setMetrics(metricArray);
            }
            metricArray.add(resultSetToMetric(jobMetricRs));
          }
        }
      }
    }
  }

  private void addPropertiesToJobExecutions(Connection connection, JobExecutionQuery query,
                                            Map<String, JobExecutionInfo> jobExecutionInfos) throws SQLException {
    if (jobExecutionInfos.size() > 0) {
      Set<String> propertyKeys = null;
      if (query.hasJobProperties()) {
        propertyKeys = Sets.newHashSet(Iterables.filter(Arrays.asList(query.getJobProperties().split(",")),
                new Predicate<String>() {
                  @Override
                  public boolean apply(String input) {
                      return !Strings.isNullOrEmpty(input);
                  }
                }));
      }
      if (propertyKeys == null || propertyKeys.size() > 0) {
        String template = String.format(JOB_PROPERTY_QUERY_STATEMENT_TEMPLATE,
                getInPredicate(jobExecutionInfos.size()));
        if (propertyKeys != null && propertyKeys.size() > 0) {
          template += String.format(" AND property_key IN (%s)", getInPredicate(propertyKeys.size()));
        }
        int index = 1;
        try (PreparedStatement jobPropertiesQueryStatement = connection.prepareStatement(template)) {
          for (String jobId : jobExecutionInfos.keySet()) {
            jobPropertiesQueryStatement.setString(index++, jobId);
          }
          if (propertyKeys != null && propertyKeys.size() > 0) {
            for (String propertyKey : propertyKeys) {
              jobPropertiesQueryStatement.setString(index++, propertyKey);
            }
          }
          try (ResultSet jobPropertiesRs = jobPropertiesQueryStatement.executeQuery()) {
            while (jobPropertiesRs.next()) {
              String jobId = jobPropertiesRs.getString("job_id");
              JobExecutionInfo jobExecutionInfo = jobExecutionInfos.get(jobId);
              StringMap jobProperties = jobExecutionInfo.getJobProperties(GetMode.NULL);
              if (jobProperties == null) {
                jobProperties = new StringMap(Maps.<String, String>newHashMap());
                jobExecutionInfo.setJobProperties(jobProperties);
              }
              Map.Entry<String, String> property = resultSetToProperty(jobPropertiesRs);
              if (propertyKeys == null || propertyKeys.contains(property.getKey())) {
                jobProperties.put(property.getKey(), property.getValue());
              }
            }
          }
        }
      }
    }
  }

  private void addTasksToJobExecutions(Connection connection, JobExecutionQuery query, Filter tableFilter,
                                       Map<String, JobExecutionInfo> jobExecutionInfos) throws SQLException {
    Map<String, Map<String, TaskExecutionInfo>> tasksExecutions =
        getTasksForJobExecutions(connection, query, tableFilter, jobExecutionInfos);
    addMetricsToTasks(connection, query, tableFilter, tasksExecutions);
    addPropertiesToTasks(connection, query, tableFilter, tasksExecutions);
    for (Map.Entry<String, Map<String, TaskExecutionInfo>> taskExecution : tasksExecutions.entrySet()) {
      JobExecutionInfo jobExecutionInfo = jobExecutionInfos.get(taskExecution.getKey());
      TaskExecutionInfoArray taskExecutionInfos = new TaskExecutionInfoArray();
      for (TaskExecutionInfo taskExecutionInfo : taskExecution.getValue().values()) {
        taskExecutionInfos.add(taskExecutionInfo);
      }
      jobExecutionInfo.setTaskExecutions(taskExecutionInfos);
    }
  }

  private Map<String, Map<String, TaskExecutionInfo>> getTasksForJobExecutions(Connection connection,
                                       JobExecutionQuery query, Filter tableFilter,
                                       Map<String, JobExecutionInfo> jobExecutionInfos) throws SQLException {
    Map<String, Map<String, TaskExecutionInfo>> taskExecutionInfos = Maps.newLinkedHashMap();
    if (query.isIncludeTaskExecutions() && jobExecutionInfos.size() > 0) {
      String template = String.format(TASK_EXECUTION_QUERY_STATEMENT_TEMPLATE,
              getInPredicate(jobExecutionInfos.size()));
      if (tableFilter.isPresent()) {
        template += " AND " + tableFilter;
      }
      int index = 1;
      try (PreparedStatement taskExecutionQueryStatement = connection.prepareStatement(template)) {
        for (String jobId : jobExecutionInfos.keySet()) {
          taskExecutionQueryStatement.setString(index++, jobId);
        }
        if (tableFilter.isPresent()) {
          tableFilter.addParameters(taskExecutionQueryStatement, index);
        }
        try (ResultSet taskRs = taskExecutionQueryStatement.executeQuery()) {
          while (taskRs.next()) {
            TaskExecutionInfo taskExecutionInfo = resultSetToTaskExecutionInfo(taskRs);
            if (!taskExecutionInfos.containsKey(taskExecutionInfo.getJobId())) {
              taskExecutionInfos.put(taskExecutionInfo.getJobId(), Maps.<String, TaskExecutionInfo>newLinkedHashMap());
            }
            taskExecutionInfos.get(taskExecutionInfo.getJobId()).put(taskExecutionInfo.getTaskId(), taskExecutionInfo);
          }
        }
      }
    }
    return taskExecutionInfos;
  }

  private void addMetricsToTasks(Connection connection, JobExecutionQuery query, Filter tableFilter,
                                 Map<String, Map<String, TaskExecutionInfo>> taskExecutionInfos) throws SQLException {
    if (query.isIncludeTaskMetrics() && taskExecutionInfos.size() > 0) {
      int index = 1;
      String template = String.format(TASK_METRIC_QUERY_STATEMENT_TEMPLATE, getInPredicate(taskExecutionInfos.size()));
      if (tableFilter.isPresent()) {
        template += " AND t." + tableFilter;
      }
      try (PreparedStatement taskMetricQueryStatement = connection.prepareStatement(template)) {
        for (String jobId : taskExecutionInfos.keySet()) {
          taskMetricQueryStatement.setString(index++, jobId);
        }
        if (tableFilter.isPresent()) {
          tableFilter.addParameters(taskMetricQueryStatement, index);
        }
        try (ResultSet taskMetricRs = taskMetricQueryStatement.executeQuery()) {
          while (taskMetricRs.next()) {
            String jobId = taskMetricRs.getString("job_id");
            String taskId = taskMetricRs.getString("task_id");
            TaskExecutionInfo taskExecutionInfo = taskExecutionInfos.get(jobId).get(taskId);
            MetricArray metricsArray = taskExecutionInfo.getMetrics(GetMode.NULL);
            if (metricsArray == null) {
              metricsArray = new MetricArray();
              taskExecutionInfo.setMetrics(metricsArray);
            }
            metricsArray.add(resultSetToMetric(taskMetricRs));
          }
        }
      }
    }
  }

  private void addPropertiesToTasks(Connection connection, JobExecutionQuery query, Filter tableFilter,
                                    Map<String, Map<String, TaskExecutionInfo>> taskExecutionInfos)
          throws SQLException {
    if (taskExecutionInfos.size() > 0) {
      Set<String> propertyKeys = null;
      if (query.hasTaskProperties()) {
          propertyKeys = Sets.newHashSet(Iterables.filter(Arrays.asList(query.getTaskProperties().split(",")),
                  new Predicate<String>() {
                      @Override
                      public boolean apply(String input) {
                          return !Strings.isNullOrEmpty(input);
                      }
                  }));
      }
      if (propertyKeys == null || propertyKeys.size() > 0) {
        String template = String.format(TASK_PROPERTY_QUERY_STATEMENT_TEMPLATE,
                getInPredicate(taskExecutionInfos.size()));
        if (propertyKeys != null && propertyKeys.size() > 0) {
          template += String.format("AND property_key IN (%s)", getInPredicate(propertyKeys.size()));
        }
        if (tableFilter.isPresent()) {
          template += " AND t." + tableFilter;
        }
        int index = 1;
        try (PreparedStatement taskPropertiesQueryStatement = connection.prepareStatement(template)) {
          for (String jobId : taskExecutionInfos.keySet()) {
            taskPropertiesQueryStatement.setString(index++, jobId);
          }
          if (propertyKeys != null && propertyKeys.size() > 0) {
            for (String propertyKey : propertyKeys) {
              taskPropertiesQueryStatement.setString(index++, propertyKey);
            }
          }
          if (tableFilter.isPresent()) {
            tableFilter.addParameters(taskPropertiesQueryStatement, index);
          }
          try (ResultSet taskPropertiesRs = taskPropertiesQueryStatement.executeQuery()) {
            while (taskPropertiesRs.next()) {
              String jobId = taskPropertiesRs.getString("job_id");
              String taskId = taskPropertiesRs.getString("task_id");
              TaskExecutionInfo taskExecutionInfo = taskExecutionInfos.get(jobId).get(taskId);
              StringMap taskProperties = taskExecutionInfo.getTaskProperties(GetMode.NULL);
              if (taskProperties == null) {
                taskProperties = new StringMap();
                taskExecutionInfo.setTaskProperties(taskProperties);
              }
              Map.Entry<String, String> property = resultSetToProperty(taskPropertiesRs);
              if (propertyKeys == null || propertyKeys.contains(property.getKey())) {
                taskProperties.put(property.getKey(), property.getValue());
              }
            }
          }
        }
      }
    }
  }

  private List<JobExecutionInfo> processQueryByJobName(Connection connection, String jobName, JobExecutionQuery query,
      Filter tableFilter)
      throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName));

    // Construct the query for job IDs by a given job name
    Filter filter = Filter.MISSING;
    String jobIdByNameQuery = JOB_ID_QUERY_BY_JOB_NAME_STATEMENT_TEMPLATE;
    if (query.hasTimeRange()) {
      // Add time range filter if applicable
      try {
        filter = constructTimeRangeFilter(query.getTimeRange());
        if (filter.isPresent()) {
          jobIdByNameQuery += " AND " + filter;
        }
      } catch (ParseException pe) {
        LOGGER.error("Failed to parse the query time range", pe);
        throw new SQLException(pe);
      }
    }

    if (!query.isIncludeJobsWithoutTasks()) {
      jobIdByNameQuery += " AND " + FILTER_JOBS_WITH_TASKS;
    }

    // Add ORDER BY
    jobIdByNameQuery += " ORDER BY created_ts DESC";

    // Query job IDs by the given job name
    List<String> jobIds = Lists.newArrayList();
    try (PreparedStatement queryStatement = connection.prepareStatement(jobIdByNameQuery)) {
      int limit = query.getLimit();
      if (limit > 0) {
        queryStatement.setMaxRows(limit);
      }
      queryStatement.setString(1, jobName);
      if (filter.isPresent()) {
        filter.addParameters(queryStatement, 2);
      }
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          jobIds.add(rs.getString(1));
        }
      }
    }
    return processQueryByIds(connection, query, tableFilter, jobIds);
  }

  private List<JobExecutionInfo> processQueryByTable(Connection connection, JobExecutionQuery query)
      throws SQLException {
    Preconditions.checkArgument(query.getId().isTable());

    Filter tableFilter = constructTableFilter(query.getId().getTable());

    String jobsWithoutTaskFilter = "";
    if (!query.isIncludeJobsWithoutTasks()) {
      jobsWithoutTaskFilter = " AND " + FILTER_JOBS_WITH_TASKS;
    }

    // Construct the query for job names by table definition
    String jobNameByTableQuery = String.format(JOB_NAME_QUERY_BY_TABLE_STATEMENT_TEMPLATE, tableFilter.getFilter(),
        jobsWithoutTaskFilter);

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

      return jobExecutionInfos;
    }
  }

  private List<JobExecutionInfo> processListQuery(Connection connection, JobExecutionQuery query)
      throws SQLException {
    Preconditions.checkArgument(query.getId().isQueryListType());

    Filter timeRangeFilter = Filter.MISSING;
    QueryListType queryType = query.getId().getQueryListType();
    String listJobExecutionsQuery;
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

    String jobsWithoutTaskFilter = "";
    if (!query.isIncludeJobsWithoutTasks()) {
      jobsWithoutTaskFilter = " WHERE " + FILTER_JOBS_WITH_TASKS;
    }
    listJobExecutionsQuery = String.format(listJobExecutionsQuery, jobsWithoutTaskFilter);
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
        List<String> jobIds = Lists.newArrayList();
        while (rs.next()) {
          jobIds.add(rs.getString(1));
        }
        return processQueryByIds(connection, query, Filter.MISSING, jobIds);
      }
    }
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
    return new AbstractMap.SimpleEntry<>(rs.getString("property_key"), rs.getString("property_value"));
  }

  private Filter constructTimeRangeFilter(TimeRange timeRange)
      throws ParseException {
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

  private static String getInPredicate(int count) {
    return StringUtils.join(Iterables.limit(Iterables.cycle("?"), count).iterator(), ",");
  }
}
