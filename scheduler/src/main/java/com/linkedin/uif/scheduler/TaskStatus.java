package com.linkedin.uif.scheduler;

/**
 * Created by ynli on 2/5/14.
 */
public enum TaskStatus {
    SUBMITTED,
    RUNNING,
    COMPLETED,
    FAILED_EXTRACT,
    FAILED_WRITE,
    FAILED_QA_CHECK,
    FAILED_COMMIT,
    CLEANEDUP
}
