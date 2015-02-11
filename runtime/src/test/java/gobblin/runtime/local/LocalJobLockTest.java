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

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.runtime.JobLock;


/**
 * Unit test for {@link LocalJobLock}.
 *
 * @author ynli
 */
@Deprecated
@Test(groups = {"gobblin.runtime.local"})
public class LocalJobLockTest {

  public void testLocalJobLock()
      throws Exception {
    final JobLock lock = new LocalJobLock();

    Thread thread1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Assert.assertTrue(lock.tryLock());
          Thread.sleep(2000);
          lock.unlock();
        } catch (Exception e) {
          // Ignored
        }
      }
    });
    thread1.start();

    Thread.sleep(1000);

    Thread thread2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Assert.assertFalse(lock.tryLock());
          Thread.sleep(2000);
          Assert.assertTrue(lock.tryLock());
          Thread.sleep(1000);
          lock.unlock();
        } catch (Exception e) {
          // Ignored
        }
      }
    });
    thread2.start();
  }
}
