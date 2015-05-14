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


/**
 * An implementation of {@link Throttler} that throttles on the number of permits allowed issued,
 * constrained by a limit.
 *
 * @author ynli
 */
public class CountBasedThrottler implements Throttler {

  private final long countLimit;
  private long count;

  public CountBasedThrottler(long countLimit) {
    this.countLimit = countLimit;
  }

  @Override
  public void start() {
    this.count = 0;
  }

  @Override
  public boolean waitForNextPermit()
      throws InterruptedException {
    return ++this.count <= this.countLimit;
  }

  @Override
  public void stop() {
    // Nothing to do
  }
}
