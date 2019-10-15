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

package org.apache.gobblin.runtime.locks;

import java.io.IOException;
import java.util.Properties;
import org.apache.gobblin.hive.HiveLockFactory;
import org.apache.gobblin.hive.HiveLockImpl;

/**
 * A lock factory that extends {@link HiveLockFactory} provide a get method for a distributed lock for a specific object
 */
public class DistributedHiveLockFactory extends HiveLockFactory {
  public DistributedHiveLockFactory(Properties properties) {
    super(properties);
  }
  public HiveLockImpl get(String name) {
    return new HiveLockImpl<ZookeeperBasedJobLock>(new ZookeeperBasedJobLock(properties, name)) {
      @Override
      public void lock() throws IOException {
        try {
          this.lock.lock();
        } catch (JobLockException e) {
          throw new IOException(e);
        }
      }

      @Override
      public void unlock() throws IOException {
        try {
          this.lock.unlock();
        } catch (JobLockException e) {
          throw new IOException(e);
        }
      }
    };
  }
}
