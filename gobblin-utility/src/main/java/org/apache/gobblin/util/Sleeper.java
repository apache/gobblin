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

package org.apache.gobblin.util;

import java.util.LinkedList;
import java.util.Queue;

import lombok.Getter;


/**
 * A class surrounding {@link Thread#sleep(long)} that allows mocking sleeps during testing.
 */
public class Sleeper {

  /**
   * A mock version of {@link Sleeper} that just register calls to sleep but returns immediately.
   */
  @Getter
  public static class MockSleeper extends Sleeper {
    private Queue<Long> requestedSleeps = new LinkedList<>();

    @Override
    public void sleep(long millis) {
      this.requestedSleeps.add(millis);
    }

    public void reset() {
      this.requestedSleeps.clear();
    }
  }

  /**
   * Equivalent to {@link Thread#sleep(long)}.
   */
  public void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
}
