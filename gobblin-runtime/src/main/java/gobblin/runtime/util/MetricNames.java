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
    public final static String CREATE_WORK_UNITS = MetricRegistry.name(BASE_NAME, "create.work.units");
    public final static String ADD_WORK_UNITS = MetricRegistry.name(BASE_NAME, "add.work.units");
    public final static String WRITE_JOB_HISTORY = MetricRegistry.name(BASE_NAME, "write.job.history");
    public final static String RUN_JOB = MetricRegistry.name(BASE_NAME, "job.run");
    public final static String COMMIT_JOB = MetricRegistry.name(BASE_NAME, "job.commit");
    public final static String CLEANUP_JOB = MetricRegistry.name(BASE_NAME, "job.cleanup");
  }

  public static class RunJobTimings {
    public static String BASE_NAME = "gobblin.job.run.timings";
    public final static String SETUP_LOCAL_JOB = MetricRegistry.name(BASE_NAME, "local.job.setup");
    public final static String RUN_WORK_UNITS = MetricRegistry.name(BASE_NAME, "run.work.units");
    public final static String SCHEDULE_WORK_UNTIS = MetricRegistry.name(BASE_NAME, "schedule.work.units");
    public final static String CLEAN_STAGING_DATA = MetricRegistry.name(BASE_NAME, "mr.clean.staging.data");
    public final static String DISTRIBUTED_CACHE = MetricRegistry.name(BASE_NAME, "populate.distributed.cache");
    public final static String SETUP_MR_JOB = MetricRegistry.name(BASE_NAME, "mr.job.setup");
    public final static String RUN_MR_JOB = MetricRegistry.name(BASE_NAME, "run.mr.job");
  }
}
