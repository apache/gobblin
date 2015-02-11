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
import java.util.Map;


/**
 * An interface for {@link JobMetrics} store.
 *
 * @author ynli
 */
public interface JobMetricsStore {

  /**
   * Put metrics in the the given {@link JobMetrics} object into the store.
   *
   * @param metrics given {@link JobMetrics} object
   * @throws IOException
   */
  public void put(JobMetrics metrics)
      throws IOException;

  /**
   * Get timestamped values of a metric identified by the given attributes.
   *
   * @param jobName job name
   * @param metricGroup metric group
   * @param id metric ID (either job ID or task ID)
   * @param name metric name
   * @return timestamped values of the metric in a {@link java.util.Map}
   *         with keys being timestamps and values being the metric values
   * @throws IOException
   */
  public Map<Long, Object> get(String jobName, JobMetrics.MetricGroup metricGroup, String id, String name,
      MetricNameSuffix suffix)
      throws IOException;

  /**
   * Get the aggregated value of a metric of a task.
   *
   * @param jobName job name
   * @param taskId task ID
   * @param name metric name
   * @param suffix metric name suffix
   * @return aggregated values of the metric of the task
   * @throws IOException
   */
  public Object aggregateOnTask(String jobName, String taskId, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException;

  /**
   * Get the aggregated value of a metric of each task in a job run.
   *
   * @param jobName job name
   * @param jobId job ID
   * @param name metric name
   * @param suffix metric name suffix
   * @return aggregated value of the metric of each task in a job run in a
   *         {@link java.util.Map} with keys being task IDs and values being
   *         aggregated metric values
   * @throws IOException
   */
  public Map<String, Object> aggregateOnTasks(String jobName, String jobId, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException;

  /**
   * Get the aggregated values of a metric of a job grouped by job runs.
   *
   * @param jobName job name
   * @param name metric name
   * @param suffix metric name suffix
   * @return aggregated values of the metric of the job grouped by job IDs in a
   *         {@link java.util.Map} with keys being job IDs and values being
   *         aggregated metric values
   * @throws IOException
   */
  public Map<String, Object> aggregateOnJobRuns(String jobName, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException;

  /**
   * Get the aggregated value of a metric of a job.
   *
   * @param jobName job name
   * @param name metric name
   * @param suffix metric name suffix
   * @param aggFunction {@link AggregationFunction}
   * @return aggregated value of the metric of the job
   * @throws IOException
   */
  public Object aggregateOnJob(String jobName, String name, MetricNameSuffix suffix, AggregationFunction aggFunction)
      throws IOException;

  /**
   * Get the aggregated value of a metric across a collection of jobs.
   *
   * @param jobNames job names
   * @param name metric name
   * @param suffix metric name suffix
   * @param aggFunction {@link AggregationFunction}
   * @return aggregated value of the metric across the collection of jobs
   * @throws IOException
   */
  public Object aggregateOnJobs(Collection<String> jobNames, String name, MetricNameSuffix suffix,
      AggregationFunction aggFunction)
      throws IOException;
}
