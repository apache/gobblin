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

/**
 * Discovers jobs to execute and generates JobSpecs for each one.
 */
public interface JobSpecMonitor {
  /**
   * Add a {@link MutableJobCatalog} that will be notified of new jobs. {@link JobMonitor}
   * will call put for any new / updated jobs, and remove for any deleted jobs.
   */
  void setJobCatalog(MutableJobCatalog jobCatalog);

}
