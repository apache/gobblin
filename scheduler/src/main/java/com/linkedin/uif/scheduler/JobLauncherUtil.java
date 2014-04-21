package com.linkedin.uif.scheduler;

/**
 * Utility class for the job scheduler and job launchers.
 *
 * @author ynli
 */
public class JobLauncherUtil {

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
     */
    public static String newTaskId(String jobId, int sequence) {
        return String.format("task_%s_%d", jobId.substring(jobId.indexOf('_') + 1), sequence);
    }
}
