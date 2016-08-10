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
package gobblin.runtime.instance;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.api.GobblinInstanceLauncher;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.job_catalog.InMemoryJobCatalog;
import gobblin.runtime.scheduler.ImmediateJobSpecScheduler;

public class StandardGobblinInstanceDriver extends DefaultGobblinInstanceDriverImpl {

  protected StandardGobblinInstanceDriver(JobCatalog jobCatalog, JobSpecScheduler jobScheduler,
      JobExecutionLauncher jobLauncher, Optional<Logger> log) {
    super(jobCatalog, jobScheduler, jobLauncher, log);
  }

  /**
   * A builder for StandardGobblinInstanceDriver instances. The goal is to be convention driven
   * rather than configuration.
   *
   * <p>Conventions:
   * <ul>
   *  <li> Logger uses the instance name as a category
   *  <li> Default implementations of JobCatalog, JobSpecScheduler, JobExecutionLauncher use the
   *       logger as their logger.
   * </ul>
   *
   */
  public static class Builder {
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);

    private Optional<GobblinInstanceLauncher> _instanceLauncher =
        Optional.<GobblinInstanceLauncher>absent();
    private Optional<String> _instanceName = Optional.absent();
    private Optional<Logger> _log = Optional.absent();
    private Optional<JobCatalog> _jobCatalog = Optional.absent();
    private Optional<JobSpecScheduler> _jobScheduler = Optional.absent();
    private Optional<JobExecutionLauncher> _jobLauncher = Optional.absent();

    public Builder(Optional<GobblinInstanceLauncher> instanceLauncher) {
      _instanceLauncher = instanceLauncher;
    }

    /** Constructor with no Gobblin instance launcher */
    public Builder() {
    }

    /** Constructor with a launcher */
    public Builder(GobblinInstanceLauncher instanceLauncher) {
      this();
      withInstanceLauncher(instanceLauncher);
    }

    public Builder withInstanceLauncher(GobblinInstanceLauncher instanceLauncher) {
      Preconditions.checkNotNull(instanceLauncher);
      _instanceLauncher = Optional.of(instanceLauncher);
      return this;
    }

    public Optional<GobblinInstanceLauncher> getInstanceLauncher() {
      return _instanceLauncher;
    }

    public String getDefaultInstanceName() {
      if (_instanceLauncher.isPresent()) {
        return _instanceLauncher.get().getInstanceName();
      }
      else {
        return StandardGobblinInstanceDriver.class.getName() + "-" +
               INSTANCE_COUNTER.getAndIncrement();
      }
    }

    public String getInstanceName() {
      if (! _instanceName.isPresent()) {
        _instanceName = Optional.of(getDefaultInstanceName());
      }
      return _instanceName.get();
    }

    public Builder withInstanceName(String instanceName) {
      _instanceName = Optional.of(instanceName);
      return this;
    }

    public Logger getDefaultLog() {
      return LoggerFactory.getLogger(getInstanceName());
    }

    public Logger getLog() {
      if (! _log.isPresent()) {
        _log = Optional.of(getDefaultLog());
      }
      return _log.get();
    }

    public Builder withLog(Logger log) {
      _log = Optional.of(log);
      return this;
    }

    public JobCatalog getDefaultJobCatalog() {
      return new InMemoryJobCatalog(Optional.of(getLog()));
    }

    public JobCatalog getJobCatalog() {
      if (! _jobCatalog.isPresent()) {
        _jobCatalog = Optional.of(getDefaultJobCatalog());
      }
      return _jobCatalog.get();
    }

    public JobSpecScheduler getDefaultJobScheduler() {
      return new ImmediateJobSpecScheduler(Optional.of(getLog()));
    }

    public JobSpecScheduler getJobScheduler() {
      if (!_jobScheduler.isPresent()) {
        _jobScheduler = Optional.of(getDefaultJobScheduler());
      }
      return _jobScheduler.get();
    }

    public Builder setJobScheduler(JobSpecScheduler jobScheduler) {
      _jobScheduler = Optional.of(jobScheduler);
      return this;
    }

    public JobExecutionLauncher getDefaultJobLauncher() {
      // FIXME
      return null;
    }

    public JobExecutionLauncher getJobLauncher() {
      if (! _jobLauncher.isPresent()) {
        _jobLauncher = Optional.of(getDefaultJobLauncher());
      }
      return _jobLauncher.get();
    }

    public Builder setJobLauncher(JobExecutionLauncher jobLauncher) {
      _jobLauncher = Optional.of(jobLauncher);
      return this;
    }

    public StandardGobblinInstanceDriver build() {
      return new StandardGobblinInstanceDriver(getJobCatalog(), getJobScheduler(), getJobLauncher(),
                                               Optional.of(getLog()));
    }
  }

}
