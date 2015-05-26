/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.util;

import com.codahale.metrics.MetricRegistry;


/**
 * Metric names for Gobblin runtime.
 */
public class MetricNames {
  public static class LauncherTimings {
    public static String BASE_NAME = "gobblin.launcher.timings";
    public final static String WORK_UNITS_CREATE = MetricRegistry.name(BASE_NAME, "work.units.create");
    public final static String WORK_UNITS_ADD = MetricRegistry.name(BASE_NAME, "work.units.add");
    public final static String JOB_HISTORY_WRITE = MetricRegistry.name(BASE_NAME, "job.history.write");
    public final static String JOB_RUN = MetricRegistry.name(BASE_NAME, "job.run");
    public final static String JOB_COMMIT = MetricRegistry.name(BASE_NAME, "job.commit");
    public final static String JOB_CLEANUP = MetricRegistry.name(BASE_NAME, "job.cleanup");
  }

  public static class RunJobTimings {
    public static String BASE_NAME = "gobblin.job.run.timings";
    public final static String JOB_LOCAL_SETUP = MetricRegistry.name(BASE_NAME, "job.local.setup");
    public final static String WORK_UNITS_RUN = MetricRegistry.name(BASE_NAME, "work.units.run");
    public final static String WORK_UNITS_SCHEDULE = MetricRegistry.name(BASE_NAME, "work.units.schedule");
    public final static String MR_STAGING_DATA_CLEAN = MetricRegistry.name(BASE_NAME, "job.mr.staging.data.clean");
    public final static String MR_DISTRIBUTED_CACHE_POPULATE = MetricRegistry.name(
        BASE_NAME, "job.mr.distributed.cache.populate");
    public final static String MR_JOB_SETUP = MetricRegistry.name(BASE_NAME, "job.mr.setup");
    public final static String MR_JOB_RUN = MetricRegistry.name(BASE_NAME, "job.mr.run");
  }
}
