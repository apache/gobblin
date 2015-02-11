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

package gobblin.runtime.local;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import gobblin.runtime.JobLock;


/**
 * A implementation of {@link gobblin.runtime.JobLock} backed by a {@link Semaphore}.
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
@Deprecated
public class LocalJobLock implements JobLock {

  private final Semaphore lock = new Semaphore(1);

  @Override
  public void lock()
      throws IOException {
    try {
      this.lock.acquire();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public void unlock()
      throws IOException {
    this.lock.release();
  }

  @Override
  public boolean tryLock()
      throws IOException {
    return this.lock.tryAcquire();
  }

  @Override
  public boolean isLocked()
      throws IOException {
    // This is not supported because it requires synchronizing all methods of
    // this class plus this method is not used at all when this class is used.
    throw new UnsupportedOperationException();
  }
}
