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
package gobblin.runtime.api;

import org.slf4j.Logger;

import com.google.common.util.concurrent.Service;

import gobblin.annotation.Alpha;

/**
 * An interface that defines a Gobblin Instance driver which knows how to monitor for Gobblin
 * {@link JobSpec}s and run them.
 * */
@Alpha
public interface GobblinInstanceDriver extends Service, JobLifecycleListenersContainer {
  JobCatalog getJobCatalog();
  /**
   * Returns a mutable instance of the job catalog
   * (if it implements the {@link MutableJobCatalog} interface). Implementation will throw
   * ClassCastException if the current catalog is not mutable.
   */
  MutableJobCatalog getMutableJobCatalog();
  JobSpecScheduler getJobScheduler();
  JobExecutionLauncher getJobLauncher();
  Logger getLog();
  Configurable getSysConfig();
}
