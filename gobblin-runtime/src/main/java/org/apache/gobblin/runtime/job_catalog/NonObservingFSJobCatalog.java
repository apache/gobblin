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
package org.apache.gobblin.runtime.job_catalog;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecNotFoundException;

/**
 * The job Catalog for file system to persist the job configuration information.
 * This implementation does not observe for file system changes
 */
public class NonObservingFSJobCatalog extends FSJobCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(NonObservingFSJobCatalog.class);

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param sysConfig
   * @throws Exception
   */
  public NonObservingFSJobCatalog(Config sysConfig)
      throws IOException {
    super(sysConfig.withValue(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
        ConfigValueFactory.fromAnyRef(ConfigurationKeys.DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)));
  }

  public NonObservingFSJobCatalog(GobblinInstanceEnvironment env) throws IOException {
    super(env);
  }

  public NonObservingFSJobCatalog(Config sysConfig, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) throws IOException{
    super(sysConfig.withValue(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
        ConfigValueFactory.fromAnyRef(ConfigurationKeys.DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)),
        parentMetricContext, instrumentationEnabled);
  }

  /**
   * Allow user to programmatically add a new JobSpec.
   * The method will materialized the jobSpec into real file.
   *
   * @param jobSpec The target JobSpec Object to be materialized.
   *                Noted that the URI return by getUri is a relative path.
   */
  @Override
  public synchronized void put(JobSpec jobSpec) {
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(jobSpec);
    try {
      long startTime = System.currentTimeMillis();
      Path jobSpecPath = getPathForURI(this.jobConfDirPath, jobSpec.getUri());
      boolean isUpdate = fs.exists(jobSpecPath);

      materializedJobSpec(jobSpecPath, jobSpec, this.fs);
      this.mutableMetrics.updatePutJobTime(startTime);
      if (isUpdate) {
        this.listeners.onUpdateJob(jobSpec);
      } else {
        this.listeners.onAddJob(jobSpec);
      }
    } catch (IOException e) {
      throw new RuntimeException("When persisting a new JobSpec, unexpected issues happen:" + e.getMessage());
    } catch (JobSpecNotFoundException e) {
      throw new RuntimeException("When replacing a existed JobSpec, unexpected issue happen:" + e.getMessage());
    }
  }

  /**
   * Allow user to programmatically delete a new JobSpec.
   * This method is designed to be reentrant.
   * @param jobURI The relative Path that specified by user, need to make it into complete path.
   */
  @Override
  public synchronized void remove(URI jobURI) {
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    try {
      long startTime = System.currentTimeMillis();
      JobSpec jobSpec = getJobSpec(jobURI);
      Path jobSpecPath = getPathForURI(this.jobConfDirPath, jobURI);

      if (fs.exists(jobSpecPath)) {
        fs.delete(jobSpecPath, false);
        this.mutableMetrics.updateRemoveJobTime(startTime);
      } else {
        LOGGER.warn("No file with URI:" + jobSpecPath + " is found. Deletion failed.");
      }

      this.listeners.onDeleteJob(jobURI, jobSpec.getVersion());
    } catch (IOException e) {
      throw new RuntimeException("When removing a JobConf. file, issues unexpected happen:" + e.getMessage());
    } catch (SpecNotFoundException e) {
      LOGGER.warn("No file with URI:" + jobURI + " is found. Deletion failed.");
    }
  }
}

