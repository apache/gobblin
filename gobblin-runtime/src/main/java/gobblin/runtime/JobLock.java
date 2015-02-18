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

package gobblin.runtime;

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
  public void lock()
      throws IOException;

  /**
   * Release the lock.
   *
   * @throws IOException
   */
  public void unlock()
      throws IOException;

  /**
   * Try locking the lock.
   *
   * @return <em>true</em> if the lock is successfully locked,
   *         <em>false</em> if otherwise.
   * @throws IOException
   */
  public boolean tryLock()
      throws IOException;

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws IOException
   */
  public boolean isLocked()
      throws IOException;
}
