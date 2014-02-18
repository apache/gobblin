package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * A implementation of {@link JobLock} backed by a {@link Semaphore}.
 *
 * <p>
 *     A {@link Semaphore} is used because the thread that unlocks the
 *     lock may not be the thread that locks the lock. Because a
 *     {@link Semaphore} is used, this implementation works only in
 *     single-node mode within a single JVM.
 * </p>
 *
 * @author ynli
 */
public class LocalJobLock implements JobLock {

    private final Semaphore lock = new Semaphore(1);

    @Override
    public void lock() throws IOException {
        try {
            this.lock.acquire();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    @Override
    public void unlock() throws IOException {
        this.lock.release();
    }

    @Override
    public synchronized boolean isLocked() throws IOException {
        return this.lock.availablePermits() == 0;
    }
}
