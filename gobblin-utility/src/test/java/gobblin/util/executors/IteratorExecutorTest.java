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

package gobblin.util.executors;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class IteratorExecutorTest {

  private final AtomicInteger nextCallCount = new AtomicInteger(0);
  private final AtomicInteger completedCount = new AtomicInteger(0);

  @Test
  public void test() throws Exception {

    TestIterator iterator = new TestIterator(5);

    final IteratorExecutor<Void> executor = new IteratorExecutor<>(iterator, 2, new ThreadFactoryBuilder().build());

    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    Future future = singleThread.submit(new Runnable() {
      @Override
      public void run() {
        try {
          executor.execute();
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    });

    // Only two threads, so exactly two tasks retrieved
    verify(2, 0);
    Assert.assertFalse(future.isDone());
    Assert.assertTrue(iterator.hasNext());

    // end one of the tasks
    iterator.endOneCallable();

    // three tasks retrieved, one completed
    verify(3, 1);
    Assert.assertFalse(future.isDone());
    Assert.assertTrue(iterator.hasNext());

    // end two tasks
    iterator.endOneCallable();
    iterator.endOneCallable();

    // 5 (all) tasks retrieved, 3 completed
    verify(5, 3);
    Assert.assertFalse(future.isDone());
    Assert.assertFalse(iterator.hasNext());

    // end two tasks
    iterator.endOneCallable();
    iterator.endOneCallable();

    // all tasks completed, check future is done
    verify(5, 5);
    Assert.assertTrue(future.isDone());

  }

  /**
   * Verify exactly retrieved tasks have been retrieved, and exactly completed tasks have completed.
   * @throws Exception
   */
  private void verify(int retrieved, int completed) throws Exception {
    for (int i = 0; i < 20; i++) {
      if (nextCallCount.get() >= retrieved || completedCount.get() >= completed) {
        break;
      }
      Thread.sleep(100);
    }
    Thread.sleep(100);
    Assert.assertEquals(nextCallCount.get(), retrieved);
    Assert.assertEquals(completedCount.get(), completed);
  }

  /**
   * Runnable iterator. Runnables block until a call to {@link #endOneCallable()}.
   */
  private class TestIterator implements Iterator<Callable<Void>> {

    private final int maxCallables;
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public TestIterator(int maxCallables) {
      this.maxCallables = maxCallables;
    }

    @Override
    public boolean hasNext() {
      return nextCallCount.get() < maxCallables;
    }

    @Override
    public Callable<Void> next() {
      nextCallCount.incrementAndGet();

      return new Callable<Void>() {
        @Override
        public Void call()
            throws Exception {

          lock.lock();
          condition.await();
          lock.unlock();

          completedCount.incrementAndGet();
          return null;
        }
      };
    }

    public void endOneCallable() {
      lock.lock();
      condition.signal();
      lock.unlock();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}
