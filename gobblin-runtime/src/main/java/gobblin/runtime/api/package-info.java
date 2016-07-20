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
/**
 * The Gobblin Execution Model.
 *
 * <p>The Gobblin launcher components:
 * <dl>
 *  <dt>Gobblin instance</dt>
 *  <dd>A self-contained context for running alike Gobblin jobs. These jobs share location where
 *  they are stored and managed, how they are scheduled, and how they are run.
 *     <dl>
 *      <dt>Gobblin instance launcher ({@link gobblin.runtime.api.GobblinInstanceLauncher})<dt>
 *      <dd>Main class instantiated by the JVM or running framework. Reads application level
 *          configurations, instantiates and runs the Gobblin instance driver.
 *          Examples: JavaMainAppLauncher (original SchedulerDaemon), AzkabanAppLauncher (original
 *          AzkabanJobLauncher).</dd>
 *      <dt>Gobblin instance driver</dt>
 *      <dd>Starts all global Gobblin services, instantiates Job Catalog, Job Launcher, and scheduler.
 *          Interprets job specs to schedule via the executor or run immediately. (Similar to original
 *          JobScheduler class).</dd>
 *     </dl>
 *  </dd>
 *  <dt>Gobblin job</dt>
 *  <dd>An execution unit that can ingest and process data from a single source.
 *    <dl>
 *      <dt>JobSpec</dt>
 *      <dd>The static part of a job, describes the job name, group, and associated configuration.</dd>
 *      <dt>JobExecution</dt>
 *      <dd>A specific execution of the job.</dd>
 *    </dl>
 *  </dd>
 *  <dt>Job catalog ({@link gobblin.runtime.api.JobCatalog})</dt>
 *  <dd>Maintains the collection of JobSpecs known to a specific Gobblin instance.
 *    <dl>
 *      <dt>JobSpec Monitor ({@link gobblin.runtime.api.JobSpecMonitor})</dt>
 *      <dd>Discovers jobs to execute and generates job specs for each one.</dd>
 *    </dl>
 *  </dd>
 *  <dt>Job Scheduler ({@link gobblin.runtime.api.JobSpecScheduler})</dt>
 *  <dd>A pluggable scheduler implementation that triggers job executions on a schedule.
 *  Examples: Quartz, pass-through.
 *  </dd>
 *  <dt>Job Launcher ({@link gobblin.runtime.api.JobExecutionLauncher})</dt>
 *  <dd>Executes a job. Examples: local job launcher (thread based), MR job launcher (mapper based),
 *  YARN launcher (container based), Helix launcher.</dd>
 * </dl>
 */
package gobblin.runtime.api;