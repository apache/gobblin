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

package gobblin.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;


/**
 * An implementation of {@link JobMetricsStore} backed by InfluxDB.
 *
 * <p>
 *     In this implementation, {@link JobMetrics} of a job form a time series.
 *     This makes it straightforward to do metric aggregation at the job level.
 *     Job-level metrics and task-level metrics, which belong to two separate
 *     metric groups, namely JOB and TASK, are mixed together but can still be
 *     queried separately due to the use of metric groups.
 * </p>
 *
 * @author ynli
 */
public class InfluxDBJobMetricsStore implements JobMetricsStore {

  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBJobMetricsStore.class);

  private static final String JOB_METRICS_DB = "gobblin_job_metrics";
  private static final String TASK_METRICS_DB = "gobblin_task_metrics";

  private static final String TIME_KEY = "time";
  private static final String JOB_ID_KEY = "jobId";
  private static final String TASK_ID_KEY = "taskId";

  private final InfluxDB influxDB;

  public InfluxDBJobMetricsStore(Properties properties) {
    String influxUrl = properties.getProperty(ConfigurationKeys.FLUXDB_URL_KEY);
    LOG.info("Connecting to InfluxDB at " + influxUrl);
    this.influxDB = InfluxDBFactory.connect(influxUrl,
        properties.getProperty(ConfigurationKeys.FLUXDB_USER_NAME_KEY, ConfigurationKeys.DEFAULT_FLUXDB_USER_NAME),
        properties
            .getProperty(ConfigurationKeys.FLUXDB_USER_PASSWORD_KEY, ConfigurationKeys.DEFAULT_FLUXDB_USER_PASSWORD));
  }

  @Override
  public void put(JobMetrics metrics)
      throws IOException {
    Map<String, Metric> metricMap = metrics.getMetrics();
    if (metricMap.isEmpty()) {
      return;
    }

    // Collect metric names/values for each metric
    for (Map.Entry<String, Metric> metric : metricMap.entrySet()) {
      List<String> metricNames = Lists.newArrayList();
      List<Object> metricValues = Lists.newArrayList();

      // Split the metric name around dot to get the 3 individual name components
      List<String> nameComponents = Splitter.on(".").splitToList(metric.getKey());
      if (nameComponents.size() < 3) {
        LOG.warn("Ignoring metric " + metric.getKey());
        continue;
      }

      JobMetrics.MetricGroup group = JobMetrics.MetricGroup.valueOf(nameComponents.get(0));

      String dbName = JOB_METRICS_DB;
      if (group == JobMetrics.MetricGroup.TASK) {
        dbName = TASK_METRICS_DB;
      }

      // Add timestamp
      metricNames.add(TIME_KEY);
      metricValues.add(System.currentTimeMillis());

      // Add job ID
      metricNames.add(JOB_ID_KEY);
      metricValues.add(metrics.getJobId());

      // Add task ID if the metric is in the TASK group
      if (group == JobMetrics.MetricGroup.TASK) {
        metricNames.add(TASK_ID_KEY);
        metricValues.add(nameComponents.get(1));
      }

      // Add other metric names and values
      metricNames.addAll(JobMetrics.getMetricNames(nameComponents.get(2), metric.getValue()));
      metricValues.addAll(JobMetrics.getMetricValue(metric.getValue()));

      if (metricNames.size() != metricValues.size()) {
        throw new IOException("Number of metric names is not equal to number of " +
            "metric values for metric set of job " + metrics.getJobId());
      }

      // Write out the metric values as a time series
      this.influxDB.write(dbName, TimeUnit.MILLISECONDS,
          new Serie.Builder(metrics.getJobName()).columns(metricNames.toArray(new String[metricNames.size()]))
              .values(metricValues.toArray(new Object[metricValues.size()])).build());
    }
  }

  @Override
  public Map<Long, Object> get(String jobName, JobMetrics.MetricGroup metricGroup, String id, String name,
      MetricNameSuffix suffix)
      throws IOException {

    Map<Long, Object> timestampedValues = Maps.newHashMap();

    String idKey = JOB_ID_KEY;
    if (metricGroup == JobMetrics.MetricGroup.TASK) {
      idKey = TASK_ID_KEY;
    }

    String dbName = JOB_METRICS_DB;
    if (metricGroup == JobMetrics.MetricGroup.TASK) {
      dbName = TASK_METRICS_DB;
    }

    String metricName = name + suffix.getSuffix();
    String query = String.format("select %s, %s from %s where %s='%s'", TIME_KEY, metricName, jobName, idKey, id);
    List<Serie> series = this.influxDB.query(dbName, query, TimeUnit.MILLISECONDS);

    // There should be only one series returned
    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return timestampedValues;
    }
    for (Map<String, Object> data : series.get(0).getRows()) {
      if (data.containsKey(metricName) && data.get(metricName) != null) {
        timestampedValues.put(((Double) data.get(TIME_KEY)).longValue(), data.get(metricName));
      }
    }

    return timestampedValues;
  }

  @Override
  public Object aggregateOnTask(String jobName, String taskId, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException {

    String aggAlias = name + suffix.getSuffix() + aggFunction.name();
    String query = String
        .format("select %s(%s) as %s from %s where %s='%s'", aggFunction.name(), name + suffix.getSuffix(), aggAlias,
            jobName, TASK_ID_KEY, taskId);
    List<Serie> series = this.influxDB.query(TASK_METRICS_DB, query, TimeUnit.MILLISECONDS);

    // There should be only one series returned
    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return null;
    }
    if (series.get(0).getRows().isEmpty()) {
      LOG.warn("No rows returned for query " + query);
      return null;
    }
    return series.get(0).getRows().get(0).get(aggAlias);
  }

  @Override
  public Map<String, Object> aggregateOnTasks(String jobName, String jobId, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException {

    Map<String, Object> values = Maps.newHashMap();

    String aggAlias = name + suffix.getSuffix() + aggFunction.name();
    String query = String
        .format("select %s, %s(%s) as %s from %s where %s='%s' group by %s", TASK_ID_KEY, aggFunction.name(),
            name + suffix.getSuffix(), aggAlias, jobName, JOB_ID_KEY, jobId, TASK_ID_KEY);
    List<Serie> series = this.influxDB.query(TASK_METRICS_DB, query, TimeUnit.MILLISECONDS);

    // There should be only one series returned
    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return values;
    }
    for (Map<String, Object> row : series.get(0).getRows()) {
      values.put(row.get(TASK_ID_KEY).toString(), row.get(aggAlias));
    }

    return values;
  }

  @Override
  public Map<String, Object> aggregateOnJobRuns(String jobName, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException {

    Map<String, Object> values = Maps.newHashMap();

    String aggAlias = name + suffix.getSuffix() + aggFunction.name();
    String query = String.format("select %s, %s(%s) as %s from %s group by %s", JOB_ID_KEY, aggFunction.name(),
        name + suffix.getSuffix(), aggAlias, jobName, JOB_ID_KEY);
    List<Serie> series = this.influxDB.query(JOB_METRICS_DB, query, TimeUnit.MILLISECONDS);

    // There should be only one series returned
    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return values;
    }
    for (Map<String, Object> row : series.get(0).getRows()) {
      values.put(row.get(JOB_ID_KEY).toString(), row.get(aggAlias));
    }

    return values;
  }

  @Override
  public Object aggregateOnJob(String jobName, String name, MetricNameSuffix suffix, AggregationFunction aggFunction)
      throws IOException {

    String aggAlias = name + suffix.getSuffix() + aggFunction.name();
    String query =
        String.format("select %s(%s) as %s from %s", aggFunction.name(), name + suffix.getSuffix(), aggAlias, jobName);
    List<Serie> series = this.influxDB.query(JOB_METRICS_DB, query, TimeUnit.MILLISECONDS);

    // There should be only one series returned
    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return null;
    }
    if (series.get(0).getRows().isEmpty()) {
      LOG.warn("No rows returned for query " + query);
      return null;
    }
    return series.get(0).getRows().get(0).get(aggAlias);
  }

  @Override
  public Object aggregateOnJobs(Collection<String> jobNames, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException {

    String aggAlias = name + suffix.getSuffix() + aggFunction.name();
    String query = String.format("select %s(%s) as %s from %s", aggFunction.name(), name + suffix.getSuffix(), aggAlias,
        Joiner.on(" merge ").join(jobNames));
    List<Serie> series = this.influxDB.query(JOB_METRICS_DB, query, TimeUnit.MILLISECONDS);

    if (series.isEmpty()) {
      LOG.warn("No series returned for query " + query);
      return null;
    }
    if (series.get(0).getRows().isEmpty()) {
      LOG.warn("No rows returned for query " + query);
      return null;
    }
    return series.get(0).getRows().get(0).get(aggAlias);
  }
}
