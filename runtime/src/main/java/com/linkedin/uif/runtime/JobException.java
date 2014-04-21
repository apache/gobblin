package com.linkedin.uif.runtime;

/**
 * A type of {@link java.lang.Exception} thrown when there is anything
 * wrong with scheduling or running a job.
 */
public class JobException extends Exception {

    public JobException(String message, Throwable t) {
        super(message, t);
    }

    public JobException(String message) {
        super(message);
    }
}
