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
 * An interface for classes that implement some throttling logic on the occurrences of some events,
 * e.g., data record extraction using an {@link gobblin.source.extractor.Extractor}.
 *
 * @author ynli
 */
public interface Throttler {

  /**
   * Start the {@link Throttler}.
   */
  public void start();

  /**
   * Wait for the next permit to proceed.
   *
   * <p>
   *   Depending on the implementation, the caller of this method may be blocked.
   * </p>
   *
   * @return {@code true} if a permit has been acquired and the caller is allowed to
   *         proceed or {@code false} otherwise and the caller should not proceed
   * @throws InterruptedException if the caller is interrupted while being blocked
   */
  public boolean waitForNextPermit() throws InterruptedException;

  /**
   * Stop the {@link Throttler}.
   */
  public void stop();
}
