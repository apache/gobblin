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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import com.google.common.collect.Lists;


/**
 * Executes tasks in an {@link Iterator}. Tasks need not be generated until they can be executed.
 * @param <T>
 */
public class IteratorExecutor<T> {

  private final CompletionService<T> completionService;
  private final int numThreads;
  private final ExecutorService executor;
  private final Iterator<Callable<T>> iterator;

  public IteratorExecutor(Iterator<Callable<T>> runnableIterator, int numThreads, ThreadFactory threadFactory) {
    this.numThreads = numThreads;
    this.iterator = runnableIterator;
    this.executor = Executors.newFixedThreadPool(numThreads, threadFactory);
    this.completionService = new ExecutorCompletionService<>(this.executor);
  }

  /**
   * Execute the tasks in the task {@link Iterator}. Blocks until all tasks are completed.
   * @return a list of completed futures.
   * @throws InterruptedException
   */
  public List<Future<T>> execute() throws InterruptedException {
    List<Future<T>> futures = Lists.newArrayList();
    int activeTasks = 0;
    while (this.iterator.hasNext()) {
      futures.add(this.completionService.submit(this.iterator.next()));
      activeTasks++;
      if (activeTasks == this.numThreads) {
        this.completionService.take();
        activeTasks--;
      }
    }
    while (activeTasks > 0) {
      this.completionService.take();
      activeTasks--;
    }

    this.completionService.poll();
    return futures;
  }

}
