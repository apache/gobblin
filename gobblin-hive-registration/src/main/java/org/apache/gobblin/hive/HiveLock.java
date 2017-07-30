/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.hive;

import java.util.concurrent.locks.Lock;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Striped;

import gobblin.util.AutoCloseableLock;


/**
 * A striped lock class for Hive databases or tables. To get a lock, use {@link #getDbLock}, {@link #getTableLock}
 * or {@link #getPartitionLock}, which returns a {@link AutoCloseableLock} object that is already locked.
 *
 * <p>
 *   Obtaining a table lock does <em>not</em> lock the database, which permits concurrent operations on different
 *   tables in the same database. Similarly, obtianing a partition lock does not lock the table or the database.
 * </p>
 */
public class HiveLock {

  private static final Joiner JOINER = Joiner.on(' ').skipNulls();

  private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

  public AutoCloseableLock getDbLock(String dbName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));

    return new AutoCloseableLock(this.locks.get(dbName));
  }

  public AutoCloseableLock getTableLock(String dbName, String tableName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));

    return new AutoCloseableLock(this.locks.get(JOINER.join(dbName, tableName)));
  }

  public AutoCloseableLock getPartitionLock(String dbName, String tableName, Iterable<String> partitionValues) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
    Preconditions.checkArgument(partitionValues.iterator().hasNext());

    return new AutoCloseableLock(this.locks.get(JOINER.join(dbName, tableName, JOINER.join(partitionValues))));
  }

}
