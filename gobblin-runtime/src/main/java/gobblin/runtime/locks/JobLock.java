/*
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

package gobblin.runtime.locks;

import java.io.Closeable;
import java.util.Properties;


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
 * @author Yinan Li
 */
public abstract class JobLock implements Closeable {
  public JobLock() {
  }

  /**
   * Initializes the lock.
   *
   * @param properties  the job properties
   * @param jobLockEventListener the listener for lock events
   * @throws JobLockException thrown if the {@link JobLock} fails to initialize
   */
  public abstract void initialize(Properties properties, JobLockEventListener jobLockEventListener)
      throws JobLockException;

  /**
   * Acquire the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  public abstract void lock()
      throws JobLockException;

  /**
   * Release the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be released
   */
  public abstract void unlock()
      throws JobLockException;

  /**
   * Try locking the lock.
   *
   * @return <em>true</em> if the lock is successfully locked,
   *         <em>false</em> if otherwise.
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  public abstract boolean tryLock()
      throws JobLockException;

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws JobLockException thrown if checking the status of the {@link JobLock} fails
   */
  public abstract boolean isLocked()
      throws JobLockException;

}
