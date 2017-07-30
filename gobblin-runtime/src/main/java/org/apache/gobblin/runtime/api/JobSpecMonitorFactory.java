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
package gobblin.runtime.api;

import java.io.IOException;


/**
 * A fact
 */
public interface JobSpecMonitorFactory {
  /**
   * Add a {@link JobSpecMonitor} that will be notified of new jobs. {@link JobSpecMonitor}
   * will call put for any new / updated jobs, and remove for any deleted jobs.
   * @param instanceDriver    the GobblinInstanceDriver managing the job catalog; this can be
   *                          used to access the environment for configuration
   * @param  jobCatalog       the job catalog to be notified for JobSpec operations.
   */
  JobSpecMonitor forJobCatalog(GobblinInstanceDriver instanceDriver, MutableJobCatalog jobCatalog) throws IOException;

}
