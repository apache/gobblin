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
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FileBasedJobLock}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class FileBasedJobLockTest {

  private FileSystem fs;
  private Path path;

  @BeforeClass
  public void setUp()
      throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.path = new Path("MRJobLockTest");
    if (!this.fs.exists(this.path)) {
      this.fs.mkdirs(this.path);
    }
  }

  public void testLocalJobLock()
      throws Exception {
    final JobLock lock = new FileBasedJobLock(this.fs, this.path.getName(), "MRJobLockTest");
    final CountDownLatch latch = new CountDownLatch(2);

    Thread thread1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Assert.assertTrue(lock.tryLock());
          Thread.sleep(2000);
          lock.unlock();
          latch.countDown();
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
          latch.countDown();
        } catch (Exception e) {
          // Ignored
        }
      }
    });
    thread2.start();

    latch.await();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (this.fs.exists(this.path)) {
      this.fs.delete(this.path, true);
    }
  }
}
