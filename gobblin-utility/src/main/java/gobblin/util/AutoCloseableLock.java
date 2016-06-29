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

package gobblin.util;

import java.util.concurrent.locks.Lock;


/**
 * An auto-closeable {@link Lock} that can be used with try-with-resources.
 * The lock is locked in the constructor and unlocked in {@link #close()}.
 *
 * <p>Usage:
 *   <pre> {@code
 *     try (AutoCloseableLock lock = new AutoCloseableLock(innerLock)) {
 *       ... do stuff
 *     }
 *   }
 *   </pre>
 * </p>
 */
public class AutoCloseableLock implements AutoCloseable {

  private final Lock lock;

  public AutoCloseableLock(Lock lock) {
    this.lock = lock;
    this.lock.lock();
  }

  @Override
  public void close() {
    this.lock.unlock();
  }
}
