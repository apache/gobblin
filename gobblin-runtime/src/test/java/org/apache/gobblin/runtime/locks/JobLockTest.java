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

package gobblin.runtime.locks;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.io.Closer;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


/**
 * Base for all {@link JobLock} unit tests.
 *
 * @author Joel Baranick
 */
@Test(groups = {"gobblin.runtime"})
public abstract class JobLockTest {
  @AfterClass
  public void tearDown() throws IOException {
    ZookeeperBasedJobLock.shutdownCuratorFramework();
  }

  protected abstract JobLock getJobLock() throws JobLockException, IOException;

  public void testLocalJobLock() throws Exception {
    Closer closer = Closer.create();
    try {
      // Set to true or false to enable debug logging in the threads
      final AtomicBoolean debugEnabled = new AtomicBoolean(true);
      final JobLock lock = closer.register(getJobLock());
      final CountDownLatch numTestsToPass = new CountDownLatch(2);

      final Lock stepsLock = new ReentrantLock();
      final AtomicBoolean thread1Locked = new AtomicBoolean(false);
      final AtomicBoolean thread2Locked = new AtomicBoolean(false);
      final Condition thread1Done = stepsLock.newCondition();
      final Condition thread2Done = stepsLock.newCondition();

      Thread thread1 = new Thread(new Runnable() {
        @Override
        public void run() {
          final Logger log = LoggerFactory.getLogger("testLocalJobLock.thread1");
          if (debugEnabled.get()) {
            org.apache.log4j.Logger.getLogger(log.getName()).setLevel(Level.DEBUG);
          }
          try {
            stepsLock.lock();
            try {
              log.debug("Acquire the lock");
              Assert.assertTrue(lock.tryLock());
              thread1Locked.set(true);
              log.debug("Notify thread2 to check the lock");
              thread1Done.signal();
              log.debug("Wait for thread2 to check the lock");
              thread2Done.await();
              log.debug("Release the file lock");
              lock.unlock();
              thread1Locked.set(false);
              log.debug("Notify and wait for thread2 to acquired the lock");
              thread1Done.signal();
              while (!thread2Locked.get()) thread2Done.await();
              Assert.assertFalse(lock.tryLock());
              log.debug("Notify thread2 that we are done with the check");
              thread1Done.signal();
            } finally {
              stepsLock.unlock();
            }

            numTestsToPass.countDown();
          } catch (Exception e) {
            log.error("error: " + e, e);
          }
        }
      }, "testLocalJobLock.thread1");
      thread1.setDaemon(true);
      thread1.start();

      Thread thread2 = new Thread(new Runnable() {
        @Override
        public void run() {
          final Logger log = LoggerFactory.getLogger("testLocalJobLock.thread2");
          if (debugEnabled.get()) {
            org.apache.log4j.Logger.getLogger(log.getName()).setLevel(Level.DEBUG);
          }
          try {
            stepsLock.lock();
            try {
              log.debug("Wait for thread1 to acquire the lock and verify we can't acquire it.");
              while (!thread1Locked.get()) thread1Done.await();
              Assert.assertFalse(lock.tryLock());
              log.debug("Notify thread1 that we are done with the check.");
              thread2Done.signal();
              log.debug("Wait for thread1 to release the lock and try to acquire it.");
              while (thread1Locked.get()) thread1Done.await();
              Assert.assertTrue(lock.tryLock());
              thread2Locked.set(true);
              thread2Done.signal();
              log.debug("Wait for thread1 to check the lock");
              thread1Done.await();

              //clean up the file lock
              lock.unlock();
            } finally {
              stepsLock.unlock();
            }

            lock.unlock();
            numTestsToPass.countDown();
          } catch (Exception e) {
            log.error("error: " + e, e);
          }
        }
      }, "testLocalJobLock.thread2");
      thread2.setDaemon(true);
      thread2.start();

      //Wait for some time for the threads to die.
      Assert.assertTrue(numTestsToPass.await(30, TimeUnit.SECONDS));
    } finally {
        closer.close();
    }
  }
}
