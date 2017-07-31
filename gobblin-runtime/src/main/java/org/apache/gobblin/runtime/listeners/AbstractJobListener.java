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

package org.apache.gobblin.runtime.listeners;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import org.apache.gobblin.runtime.JobContext;


/**
 * An abstract implementation of {@link JobListener} which ensures that
 * subclasses do not have to implement all lifecycle events.
 *
 * @author Joel Baranick
 */
public abstract class AbstractJobListener implements JobListener {
  private final Optional<Logger> _log;

  public AbstractJobListener(Optional<Logger> log) {
    _log = log;
  }

  public AbstractJobListener() {
    this(Optional.<Logger>absent());
  }

  @Override
  public void onJobPrepare(JobContext jobContext) throws Exception {
    if (_log.isPresent()) {
      _log.get().info("jobPrepare: " + jobContext);
    }
  }

  @Override
  public void onJobStart(JobContext jobContext) throws Exception {
    if (_log.isPresent()) {
      _log.get().info("jobStart: " + jobContext);
    }
  }

  @Override
  public void onJobCompletion(JobContext jobContext) throws Exception {
    if (_log.isPresent()) {
      _log.get().info("jobCompletion: " + jobContext);
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext) throws Exception {
    if (_log.isPresent()) {
      _log.get().info("jobCancellation: " + jobContext);
    }
  }

  @Override
  public void onJobFailure(JobContext jobContext) throws Exception {
    if (_log.isPresent()) {
      _log.get().info("jobFailure: " + jobContext);
    }
  }

  protected Optional<Logger> getLog() {
    return _log;
  }
}
