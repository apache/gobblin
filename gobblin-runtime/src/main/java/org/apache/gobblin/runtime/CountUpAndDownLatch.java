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

package org.apache.gobblin.runtime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A {@link CountDownLatch} that allows counting up. Backed by a {@link Phaser}.
 */
class CountUpAndDownLatch extends CountDownLatch {

  private final Phaser phaser;

  public CountUpAndDownLatch(int count) {
    super(0);
    this.phaser = new Phaser(count) {
      @Override
      protected boolean onAdvance(int phase, int registeredParties) {
        // Need to override onAdvance because phaser by default terminates whenever registered parties reaches 0
        return false;
      }
    };
  }

  @Override
  public void await() throws InterruptedException {
    int phase = getPhase();
    this.phaser.awaitAdvance(phase);
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    try {
      int phase = getPhase();
      this.phaser.awaitAdvanceInterruptibly(phase, timeout, unit);
      return true;
    } catch (TimeoutException te) {
      return false;
    }
  }

  private int getPhase() {
    int phase = this.phaser.register();
    this.phaser.arriveAndDeregister();
    return phase;
  }

  @Override
  public void countDown() {
    this.phaser.arriveAndDeregister();
  }

  public void countUp() {
    this.phaser.register();
  }

  @Override
  public long getCount() {
    return this.phaser.getUnarrivedParties();
  }

  public long getRegisteredParties() {
    return this.phaser.getRegisteredParties();
  }

  @Override
  public String toString() {
    return "Unarrived parties: " + this.phaser.getUnarrivedParties();
  }
}
