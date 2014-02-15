package com.linkedin.uif.scheduler;

import java.io.IOException;

/**
 * A class for claiming exclusive right to proceed for each scheduled
 * run of a job.
 *
 * <p>
 *     By acquiring a {@link JobLock} before a scheduled run of a job
 *     can proceed, it is guaranteed that no more than one instance of
 *     a job is running at any time.
 * </p>
 *
 * @author ynli
 */
public interface JobLock {

    /**
     * Acquire the lock.
     *
     * @throws IOException
     */
    public void lock() throws IOException;

    /**
     * Release the lock.
     *
     * @throws IOException
     */
    public void unlock() throws IOException;

    /**
     * Check if the lock is locked.
     *
     * @return if the lock is locked
     * @throws IOException
     */
    public boolean isLocked() throws IOException;
}
