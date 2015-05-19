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
 * An interface for classes that implement some logic limiting on the occurrences of some events,
 * e.g., data record extraction using an {@link gobblin.source.extractor.Extractor}.
 *
 * @author ynli
 */
public interface Limiter {

  /**
   * Supported types of {@link Limiter}s.
   */
  public enum Type {
    /**
     * For {@link RateBasedLimiter}.
     */
    RATE_BASED("rate"),

    /**
     * For {@link TimeBasedLimiter}.
     */
    TIME_BASED("time"),

    /**
     * For {@link CountBasedLimiter}.
     */
    COUNT_BASED("count"),

    /**
     * For {@link PoolBasedLimiter}.
     */
    POOL_BASED("pool");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }

    /**
     * Get a {@link Limiter.Type} for the given name.
     *
     * @param name the given name
     * @return a {@link Limiter.Type} for the given name
     */
    public static Type forName(String name) {
      if (name.equalsIgnoreCase(RATE_BASED.name)) {
        return RATE_BASED;
      }
      if (name.equalsIgnoreCase(TIME_BASED.name)) {
        return TIME_BASED;
      }
      if (name.equalsIgnoreCase(COUNT_BASED.name)) {
        return COUNT_BASED;
      }
      if (name.equalsIgnoreCase(POOL_BASED.name)) {
        return POOL_BASED;
      }
      throw new IllegalArgumentException("No Limiter implementation available for name: " + name);
    }
  }

  /**
   * Start the {@link Limiter}.
   *
   * See {@link #stop()}
   */
  public void start();

  /**
   * Acquire a given number of permits.
   *
   * <p>
   *   Depending on the implementation, the caller of this method may be blocked.
   *   It is also up to the caller to decide how to deal with the return value.
   * </p>
   *
   * @param permits number of permits to get
   * @return {@code true} if the permits have been acquired successfully or {@code false} otherwise
   * @throws InterruptedException if the caller is interrupted while being blocked
   */
  public boolean acquirePermits(int permits) throws InterruptedException;

  /**
   * Release a given number of permits.
   *
   * <p>
   *   This operation may or may not be supported, depending on the implementations.
   * </p>
   *
   * @param permits number of permits to release
   */
  public void releasePermits(int permits);

  /**
   * Stop the {@link Limiter}.
   *
   * See {@link #start()}
   */
  public void stop();
}
