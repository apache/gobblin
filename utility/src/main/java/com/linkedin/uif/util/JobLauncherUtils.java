/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.util;

/**
 * Utility class for the job scheduler and job launchers.
 *
 * @author ynli
 */
public class JobLauncherUtils {

    /**
     * Create a new job ID.
     *
     * @param jobName Job name
     * @return new job ID
     */
    public static String newJobId(String jobName) {
        // Job ID in the form of job_<job_id_suffix>
        // <job_id_suffix> is in the form of <job_name>_<current_timestamp>
        String jobIdSuffix = String.format("%s_%d", jobName, System.currentTimeMillis());
        return "job_" + jobIdSuffix;
    }

    /**
     * Create a new task ID for the job with the given job ID.
     *
     * @param jobId Job ID
     * @param sequence Task sequence number
     */
    public static String newTaskId(String jobId, int sequence) {
        return String.format("task_%s_%d", jobId.substring(jobId.indexOf('_') + 1), sequence);
    }
}
