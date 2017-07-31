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

package org.apache.gobblin.metrics.performance;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;


/**
 * {@link Callable} that keeps incrementing a count up to a max_count. At each iteration, it calls a custom
 * callback.
 */
@Slf4j
public class Incrementer implements Callable<Long> {

  private final long max_count;
  private final Runnable runnable;
  private final long logInterval;

  /**
   * @param max_count number of iterations of increment to run in call() method.
   * @param runnable the {@link Runnable#run} method will be called for each iteration.
   */
  public Incrementer(long max_count, Runnable runnable) {
    this.max_count = max_count;
    this.runnable = runnable;
    this.logInterval = max_count / 10;
  }

  @Override public Long call() throws Exception {

    long count = 0;

    long startTime = System.currentTimeMillis();

    long nextLog = this.logInterval;

    while(count < this.max_count) {
      if(count >= nextLog) {
        log.info(String.format("Incremented %d of %d times.", count, this.max_count));
        nextLog += this.logInterval;
      }
      onIteration();
      count++;
    }

    long endTime = System.currentTimeMillis();

    return endTime - startTime;
  }

  protected void onIteration() {
    this.runnable.run();
  }

}
