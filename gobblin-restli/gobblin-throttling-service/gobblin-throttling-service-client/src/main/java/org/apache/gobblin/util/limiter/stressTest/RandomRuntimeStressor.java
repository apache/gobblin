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

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link Stressor} that repeatedly requests 1 permits from {@link Limiter} for a random number of seconds between
 * 1 and 181.
 */
@Slf4j
public class RandomRuntimeStressor extends RandomDelayStartStressor {

  @Override
  public void configure(Configuration configuration) {
    // Do nothing
  }

  @Override
  public void doRun(Limiter limiter) throws InterruptedException {
    long runForSeconds = new Random().nextInt(180) + 1;
    long endTime = System.currentTimeMillis() + runForSeconds * 1000;
    while (System.currentTimeMillis() < endTime) {
      limiter.acquirePermits(1);
    }
  }
}
