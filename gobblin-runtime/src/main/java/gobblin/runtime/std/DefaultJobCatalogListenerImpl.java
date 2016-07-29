/*
 * Copyright (C) 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.std;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;

/**
 * Default NOOP implementation for {@link JobCatalogListener}. It can log the callbacks. Other
 * implementing classes can use this as a base to override only the methods they care about.
 */
public class DefaultJobCatalogListenerImpl implements JobCatalogListener {
  protected final Optional<Logger> _log;

  /**
   * Constructor
   * @param log   if no log is specified, the logging will be done. Logging level is INFO.
   */
  public DefaultJobCatalogListenerImpl(Optional<Logger> log) {
    _log = log;
  }

  public DefaultJobCatalogListenerImpl(Logger log) {
    this(Optional.of(log));
  }

  /** Constructor with no logging */
  public DefaultJobCatalogListenerImpl() {
    this(Optional.<Logger>absent());
  }

  /** {@inheritDoc} */
  @Override public void onAddJob(JobSpec addedJob) {
    if (_log.isPresent()) {
      _log.get().info("New JobSpec detected: " + addedJob.toShortString());
    }
  }

  /** {@inheritDoc} */
  @Override public void onDeleteJob(JobSpec deletedJob) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec deleted: " + deletedJob.toShortString());
    }
  }

  /** {@inheritDoc} */
  @Override public void onUpdateJob(JobSpec originalJob, JobSpec updatedJob) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec changed: " + originalJob.toShortString() + " --> " +
             updatedJob.toShortString());
    }
  }

}
