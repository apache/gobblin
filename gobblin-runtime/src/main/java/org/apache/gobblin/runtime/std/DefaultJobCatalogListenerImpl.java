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
package gobblin.runtime.std;

import java.net.URI;

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
  @Override public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec deleted: " + deletedJobURI + "/" + deletedJobVersion);
    }
  }

  /** {@inheritDoc} */
  @Override public void onUpdateJob(JobSpec updatedJob) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec changed: " + updatedJob.toShortString());
    }
  }

}
