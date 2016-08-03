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

package gobblin.runtime;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestUtils {

  /**
   * Block until a condition is true or a timeout is met. Uses an exponential backoff with given initial wait.
   * @return true if the condition was met, false if timeout.
   * @throws Exception
   */
  public static boolean awaitTrue(Callable<Boolean> condition, long timeout, long initialWait, TimeUnit timeUnit)
    throws Exception {

    if (initialWait <= 0) {
      initialWait = 1;
    }

    long currentWait = initialWait;
    long totalWait = 0;
    do {
      Thread.sleep(timeUnit.toMillis(currentWait));
      totalWait += currentWait;
      if (condition.call()) {
        return true;
      }
      currentWait *= 2;
    } while (totalWait < timeout);

    return false;
  }

}
