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
/**
 * The Gobblin Execution Model.
 *
 * <p>The Gobblin launcher components:
 * <dl>
 *  <dt>Gobblin instance</dt>
 *  <dd>A self-contained context for running alike Gobblin jobs. These jobs share location where
 *  they are stored and managed, how they are scheduled, and how they are run.
 *     <dl>
 *      <dt>Gobblin instance driver ({@link gobblin.runtime.api.GobblinInstanceDriver})</dt>
 *      <dd>Starts all global Gobblin services, instantiates Job Catalog, Job Launcher, and scheduler.
 *          Interprets job specs to schedule via the executor or run immediately. (Similar to original
 *          JobScheduler class).</dd>
 *     </dl>
 *      <dt>Gobblin instance launcher ({@link gobblin.runtime.api.GobblinInstanceLauncher})<dt>
 *      <dd>Main class instantiated by the JVM or running framework. Reads application level
 *          configurations, instantiates and runs the Gobblin instance driver.
 *          Examples: JavaMainAppLauncher (original SchedulerDaemon), AzkabanAppLauncher (original
 *          AzkabanJobLauncher).</dd>
 *  </dd>
 *  <dt>Gobblin job</dt>
 *  <dd>An execution unit that can ingest and process data from a single source.
 *    <dl>
 *      <dt>JobSpec</dt>
 *      <dd>The static part of a job, describes the job name, group, and associated configuration.</dd>
 *      <dt>JobExecution</dt>
 *      <dd>A specific execution of the job.</dd>
 *      <dt>JobExecution Driver</dt>
 *      <dd>Executes the job and its tasks. This can be done locally in a thread-pool,
 *          or as a M/R job, as a Yarn job using the Helix task execution framework, etc. This
 *          concept replaces the previous {@link gobblin.runtime.JobLauncher}. </dd>
 *      <dt>JobExecution Launcher ({@link gobblin.runtime.api.JobExecutionLauncher})</dt>
 *      <dd>Instantiates the JobExecution Driver. The driver may be instantiated locally, or it
 *      can be launched in a remote container (similarly to Oozie). JobExecutionLauncher should not
 *      be confused with the legacy {@link gobblin.runtime.JobLauncher}. Essentially, we decided to
 *      roughly split the JobLauncher into JobExecutionLauncher and a JobExecutionDriver. This
 *      allows us to abstract the aspect of instantiating and running the JobExecutionDriver. Thus,
 *      we have the option of running the driver locally or remotely. </dd>
 *    </dl>
 *  </dd>
 *  <dt>Job catalog ({@link gobblin.runtime.api.JobCatalog})</dt>
 *  <dd>Maintains the collection of JobSpecs known to a specific Gobblin instance.
 *    <dl>
 *      <dt>JobSpec Monitor ({@link gobblin.runtime.api.JobSpecMonitorFactory})</dt>
 *      <dd>Discovers jobs to execute and generates job specs for each one.</dd>
 *    </dl>
 *  </dd>
 *  <dt>Job Scheduler ({@link gobblin.runtime.api.JobSpecScheduler})</dt>
 *  <dd>A pluggable scheduler implementation that triggers job executions on a schedule.
 *  Examples: Quartz, pass-through.
 *  </dd>
 * </dl>
 *
 * <p>Overall, in the current design, Gobblin defines an execution hierarchy: a Gobblin Instance
 * runs zero, one or more Gobblin Jobs which consist of zero, one or more tasks. For each of the
 * levels in the hierarchy, there is a Driver which is responsible of running the children
 * components and a Launcher which is responsible for instantiating and invoking the Driver.
 */
package gobblin.runtime.api;