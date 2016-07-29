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
package gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.List;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.util.JobCatalogUtils;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.filesystem.PathAlterationMonitor;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;
import gobblin.runtime.job_catalog.JobCatalogListenersList.AddJobCallback;
import gobblin.runtime.job_catalog.JobCatalogListenersList.UpdateJobCallback;
import gobblin.runtime.job_catalog.JobCatalogListenersList.DeleteJobCallback;


/**
 * The job Catalog for file system to persist the job configuration information.
 * This implementation has no support for caching.
 */
public class FSJobCatalog implements JobCatalog {
  //  public Path jobConfDirPath;
  public final Properties _properties;

  private final JobCatalogListenersList listeners;
  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalog.class);
  private final Path jobConfDirPath;

  // A monitor for changes to job conf files for general FS
  // This embedded monitor is monitoring job configuration files instead of JobSpec Object.
  public final PathAlterationMonitor pathAlterationMonitor;

  /**
   * SINGLEJOB means load job configuration for single job.
   * BATCHJOB means load a list of job configuration from a directory.
   */
  public enum Action {
    SINGLEJOB, BATCHJOB
  }

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param properties
   * @throws Exception
   */
  public FSJobCatalog(Properties properties)
      throws Exception {
    this._properties = properties;
    this.jobConfDirPath = new Path(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    this.listeners = new JobCatalogListenersList(Optional.of(LOGGER));

    long pollingInterval = Long.parseLong(
        this._properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
            Long.toString(ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)));
    this.pathAlterationMonitor = new PathAlterationMonitor(pollingInterval);

    // Start the monitor immediately the FSJobCatalog is created so that
    // all newly-created file will be reported.
    startGenericFSJobConfigMonitor();
  }

  /**
   * Fetch all the job files under the jobConfDirPath
   * @return A collection of JobSpec
   */
  @Override
  public synchronized List<JobSpec> getJobs() {
    return JobCatalogUtils.loadJobConfigHelper(this.jobConfDirPath, Action.BATCHJOB, new Path("dummy Path"));
  }

  /**
   * Fetch single job file based on its URI,
   * return null requested URI not existed
   * requires null checking
   * @param uri
   * @return
   */
  @Override
  public synchronized JobSpec getJobSpec(URI uri) {
    Path targetJobSpecPath = new Path(uri);

    List<JobSpec> resultJobSpecList =
        JobCatalogUtils.loadJobConfigHelper(this.jobConfDirPath, Action.SINGLEJOB, targetJobSpecPath);
    if (resultJobSpecList == null || resultJobSpecList.size() == 0) {
      LOGGER.warn("No JobSpec with URI:" + uri + " is found.");
      return null;
    } else {
      return resultJobSpecList.get(0);
    }
  }

  /**
   * For each new coming JobCatalogListener, react accordingly to add all existing JobSpec.
   * @param jobListener
   */
  @Override
  public synchronized void addListener(JobCatalogListener jobListener) {
    Preconditions.checkNotNull(jobListener);

    this.listeners.addListener(jobListener);

    List<JobSpec> currentJobSpecList = this.getJobs();
    if (currentJobSpecList == null || currentJobSpecList.size() == 0) {
      return;
    } else {
      for (JobSpec jobSpecEntry : currentJobSpecList) {
        AddJobCallback addJobCallback = new AddJobCallback(jobSpecEntry);
        this.listeners.callbackOneListener(addJobCallback, jobListener);
      }
    }
  }

  @Override
  public synchronized void removeListener(JobCatalogListener jobListener) {
    this.listeners.removeListener(jobListener);
  }

  /**
   * return the path that it controls.
   * @return
   */

  public Path getPath() {
    return this.jobConfDirPath;
  }

  /**
   * Monitor specially for file system as it is stateful storage system, which might result in monitoring loop.
   * Note that here the entity for monitoring is job conf. file instead of JobSpec Object.
   */
  private void startGenericFSJobConfigMonitor()
      throws Exception {
    FSPathAlterationListenerAdaptor configFilelistener = new FSPathAlterationListenerAdaptor(this.jobConfDirPath);
    JobCatalogUtils.addPathAlterationObserver(this.pathAlterationMonitor, configFilelistener, this.jobConfDirPath);
    this.pathAlterationMonitor.start();
  }

  private class FSPathAlterationListenerAdaptor extends PathAlterationListenerAdaptor {
    Path jobConfigFileDirPath;

    FSPathAlterationListenerAdaptor(Path jobConfigFileDirPath) {
      this.jobConfigFileDirPath = jobConfigFileDirPath;
    }

    /**
     * Transform the event triggered by file creation into JobSpec Creation for Driver (One of the JobCatalogListener )
     * Create a new JobSpec object and notify each of member inside JobCatalogListenersList
     * @param path
     */
    @Override
    public void onFileCreate(Path path) {
      JobSpec newJobSpec = JobCatalogUtils.loadJobConfigHelper(jobConfDirPath, Action.SINGLEJOB, path).get(0);
      AddJobCallback addJobCallback = new AddJobCallback(newJobSpec);
      listeners.callbackAllListeners(addJobCallback);
    }

    /**
     * In the call to {@link UpdateJobCallback}, it is hard to retrieve oldJobSpec without caching layer suppport.
     * Here simply passed the null.
     * @param path
     */
    @Override
    public void onFileChange(Path path) {
      JobSpec updatedJobSpec = JobCatalogUtils.loadJobConfigHelper(jobConfDirPath, Action.SINGLEJOB, path).get(0);
      UpdateJobCallback updateJobCallback = new UpdateJobCallback(null, updatedJobSpec);
      listeners.callbackAllListeners(updateJobCallback);
    }

    /**
     * For already deleted job configuration file, the only identifier is path
     * it doesn't make sense to loadJobConfig Here.
     * @param path
     */
    @Override
    public void onFileDelete(Path path) {
      JobSpec deletedJobSpec = JobSpec.builder(path.toUri()).build();
      DeleteJobCallback deleteJobCallback = new DeleteJobCallback(deletedJobSpec);
      listeners.callbackAllListeners(deleteJobCallback);
    }
  }
}
