package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A implementation of {@link JobLock} backed by a {@link ReentrantLock}.
 *
 * <p>
 *     Because a {@link ReentrantLock} is used, this implementation
 *     works only in single-node mode within a single JVM.
 * </p>
 *
 * @author ynli
 */
public class LocalJobLock implements JobLock {

    // We use the ReentrantLock instead of Lock because we need
    // some method of ReentrantLock that is not available in Lock
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public void lock() throws IOException {
        this.lock.lock();
    }

    @Override
    public void unlock() throws IOException {
        this.lock.unlock();
    }

    @Override
    public boolean isLocked() throws IOException {
        return this.lock.isLocked();
    }
}
