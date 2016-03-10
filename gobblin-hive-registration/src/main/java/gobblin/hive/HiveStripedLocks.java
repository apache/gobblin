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

package gobblin.hive;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Striped;


/**
 * A striped lock class for Hive databases or tables. To get a lock, use {@link #getDbLock} or {@link #getTableLock},
 * which returns an {@link AutoCloseable} object which returns the lock on close. The recommended usage is:
 *
 * <pre>
 *   HiveStripedLocks locks = new HiveStripedLocs();
 *   try (HiveLock lock = locks.getDbLock("db")) {
 *     ... do stuff
 *   }
 * </pre>
 *
 * This class uses a {@link Striped} object internally.
 *
 * @author ziliu
 */
public class HiveStripedLocks {
  private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

  public HiveLock getDbLock(String dbName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));

    Lock dbLock = locks.get(dbName);
    return new HiveLock(dbLock);
  }

  public HiveLock getTableLock(String dbName, String tableName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));

    Lock dbLock = locks.get(dbName);
    Lock tableLock = locks.get(dbName + "." + tableName);
    return new HiveLock(dbLock, tableLock);
  }

  /**
   * A lock class that internally wraps a list of {@link Lock}s.
   *
   * Supports operations {@link #lock()} and {@link #unlock()}.
   */
  public static class HiveLock implements AutoCloseable {
    private final List<Lock> locks;

    private HiveLock(Lock... locks) {
      this.locks = Arrays.asList(locks);
      this.lock();
    }

    private void lock() {
      for (Lock lock : this.locks) {
        lock.lock();
      }
    }

    private void unlock() {
      for (int i = this.locks.size() - 1; i >= 0; i--) {
        this.locks.get(i).unlock();
      }
    }

    @Override public void close() {
      unlock();
    }
  }
}
