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
 * A interface for claiming exclusive right to proceed for each scheduled
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
public interface JobLock extends Closeable {

  /**
   * Acquire the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  void lock()
      throws JobLockException;

  /**
   * Release the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be released
   */
  void unlock()
      throws JobLockException;

  /**
   * Try locking the lock.
   *
   * @return <em>true</em> if the lock is successfully locked,
   *         <em>false</em> if otherwise.
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  boolean tryLock()
      throws JobLockException;

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws JobLockException thrown if checking the status of the {@link JobLock} fails
   */
  boolean isLocked()
      throws JobLockException;

}
