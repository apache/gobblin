/*
 *
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

package gobblin.runtime.util;

/**
 * Event names for Gobblin runtime timings.
 */
public class TimingEventNames {

  public static class LauncherTimings {
    public static final String FULL_JOB_EXECUTION = "FullJobExecutionTimer";
    public static final String WORK_UNITS_CREATION = "WorkUnitsCreationTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String JOB_PREPARE = "JobPrepareTimer";
    public static final String JOB_START = "JobStartTimer";
    public static final String JOB_RUN = "JobRunTimer";
    public static final String JOB_COMMIT = "JobCommitTimer";
    public static final String JOB_CLEANUP = "JobCleanupTimer";
    public static final String JOB_CANCEL = "JobCancelTimer";
    public static final String JOB_COMPLETE = "JobCompleteTimer";
    public static final String JOB_FAILED = "JobFailedTimer";
  }

  public static class RunJobTimings {
    public static final String JOB_LOCAL_SETUP = "JobLocalSetupTimer";
    public static final String WORK_UNITS_RUN = "WorkUnitsRunTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String MR_STAGING_DATA_CLEAN = "JobMrStagingDataCleanTimer";
    public static final String MR_DISTRIBUTED_CACHE_SETUP = "JobMrDistributedCacheSetupTimer";
    public static final String MR_JOB_SETUP = "JobMrSetupTimer";
    public static final String MR_JOB_RUN = "JobMrRunTimer";
    public static final String HELIX_JOB_SUBMISSION = "JobHelixSubmissionTimer";
    public static final String HELIX_JOB_RUN = "JobHelixRunTimer";
  }
}
