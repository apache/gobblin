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

package org.apache.gobblin.util.limiter.stressTest;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.gobblin.util.limiter.Limiter;


/**
 * A {@link Stressor} that sleeps for a random delay between 0 and 30 seconds before starting.
 */
public abstract class RandomDelayStartStressor implements Stressor {

  @Override
  public void configure(Configuration configuration) {

  }

  @Override
  public void run(Limiter limiter) throws InterruptedException {
    long delayStartSeconds = new Random().nextInt(30);
    Thread.sleep(delayStartSeconds * 1000);
    doRun(limiter);
  }

  /**
   * Run the actual logic in the {@link Stressor}.
   */
  public abstract void doRun(Limiter limiter) throws InterruptedException;
}
