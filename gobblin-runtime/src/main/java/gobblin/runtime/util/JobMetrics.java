/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.Tag;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;


/**
 * An extension to {@link GobblinMetrics} specifically for job runs.
 *
 * @author ynli
 */
public class JobMetrics extends GobblinMetrics {

  protected final String jobName;

  private static final Configuration HADOOP_CONFIGURATION = new Configuration();

  protected JobMetrics(JobState job) {
    super(name(job), null, tagsForJob(job));
    this.jobName = job.getJobName();

  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobName job name
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobName, String jobId) {
    return get(new JobState(jobName, jobId));
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobId) {
    return get(null, jobId);
  }

  /**
   * Get a {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * @param jobState the given {@link JobState} instance
   * @return a {@link JobMetrics} instance
   */
  public static synchronized JobMetrics get(JobState jobState) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    String name = name(jobState);
    if (!registry.containsKey(name)) {
      registry.putIfAbsent(name, new JobMetrics(jobState));
    }
    return (JobMetrics) registry.get(name);
  }

  /**
   * Remove the {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * <p>
   *   Removing a {@link JobMetrics} instance for a job will also remove the {@link TaskMetrics}s
   *   of every tasks of the job.
   * </p>
   * @param jobState the given {@link JobState} instance
   */
  public synchronized static void remove(JobState jobState) {
    remove(name(jobState));
    for (TaskState taskState : jobState.getTaskStates()) {
      TaskMetrics.remove(taskState);
    }
  }

  private static String name(JobState jobState) {
    return "gobblin.metrics." + jobState.getJobId();
  }

  private static List<Tag<?>> tagsForJob(JobState jobState) {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.add(new Tag<String>("jobName", jobState.getJobName() == null ? "" : jobState.getJobName()));
    tags.add(new Tag<String>("jobId", jobState.getJobId()));

    String clusterIdentifierTag = getClusterIdentifierTag();
    if (StringUtils.isNotBlank(clusterIdentifierTag)) {
      tags.add(new Tag<String>("clusterIdentifier", clusterIdentifierTag));
    }

    tags.addAll(getCustomTagsFromState(jobState));
    return tags;
  }

  /**
   *
   * Builds the clusterIdentifier tag.
   *
   * <p><b>MapReduce mode</b>
   * Gets the value for "yarn.resourcemanager.address" from {@link Configuration} excluding the port number.
   * If "yarn.resourcemanager.address" is not set, (possible in Hadoop1), falls back to "mapreduce.jobtracker.address"</p>
   *
   *<p><b>Standalone mode (outside of hadoop)</b>
   * returns the Hostname of {@link InetAddress#getLocalHost()}</p>
   *
   */
  public static String getClusterIdentifierTag() {

    // ResourceManager address in Hadoop2
    String clusterIdentifier = HADOOP_CONFIGURATION.get("yarn.resourcemanager.address");

    // If job is running on Hadoop1 use jobtracker address
    if (clusterIdentifier == null) {
      clusterIdentifier = HADOOP_CONFIGURATION.get("mapreduce.jobtracker.address");
    }

    clusterIdentifier = ClustersNames.getInstance().getClusterName(clusterIdentifier);

    // If job is running outside of Hadoop (Standalone) use hostname
    // If clusterIdentifier is localhost or 0.0.0.0 use hostname
    if (clusterIdentifier == null
        || StringUtils.startsWithIgnoreCase(clusterIdentifier, "localhost")
        || StringUtils.startsWithIgnoreCase(clusterIdentifier, "0.0.0.0")) {
      try {
        clusterIdentifier = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        // Do nothing. Tag will not be generated
      }
    }

    return clusterIdentifier;

  }

}
