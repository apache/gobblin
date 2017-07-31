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

package org.apache.gobblin.restli.throttling;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A wrapper around a {@link TokenBucket} that returns different number of tokens following an internal heuristic.
 *
 * The heuristic is as follows:
 * * The calling process specifies an ideal and minimum number of token it requires, as well as a timeout.
 * * If there is a large number of tokens stored (i.e. underutilization), this class may return more than the requested
 *   ideal number of tokens (up to 1/2 of the stored tokens). This reduces unnecessary slowdown when there is no
 *   contention.
 * * The object computes a target timeout equal to the minimum time needed to fulfill the minimum requested permits
 *   (according to the configured qps) plus a {@link #baseTimeout}.
 * * The object will return as many permits as it can using that timeout, bounded by minimum and desired number of permits.
 */
@Slf4j
public class DynamicTokenBucket {

  @VisibleForTesting
  @Getter
  private final TokenBucket tokenBucket;
  private final long baseTimeout;

  /**
   * @param qps the average qps desired.
   * @param fullRequestTimeoutMillis max time to fully satisfy a token request. This is generally a small timeout, on the
   *                                 order of the network latency (e.g. ~100 ms).
   * @param maxBucketSizeMillis maximum number of unused tokens that can be stored during under-utilization time, in
   *                            milliseconds. The actual tokens stored will be 1000 * qps * maxBucketSizeMillis.
   */
  DynamicTokenBucket(long qps, long fullRequestTimeoutMillis, long maxBucketSizeMillis) {
    this.tokenBucket = new TokenBucket(qps, maxBucketSizeMillis);
    this.baseTimeout = fullRequestTimeoutMillis;
  }

  /**
   * Request tokens.
   * @param requestedPermits the ideal number of tokens to acquire.
   * @param minPermits the minimum number of tokens useful for the calling process. If this many tokens cannot be acquired,
   *                   the method will return 0 instead,
   * @param timeoutMillis the maximum wait the calling process is willing to wait for tokens.
   * @return the number of allocated tokens.
   */
  public long getPermits(long requestedPermits, long minPermits, long timeoutMillis) {

    try {
      long storedTokens = this.tokenBucket.getStoredTokens();

      long eagerTokens = storedTokens / 2;
      if (eagerTokens > requestedPermits && this.tokenBucket.getTokens(eagerTokens, 0, TimeUnit.MILLISECONDS)) {
        return eagerTokens;
      }

      long millisToSatisfyMinPermits = (long) (minPermits / this.tokenBucket.getTokensPerMilli());
      if (millisToSatisfyMinPermits > timeoutMillis) {
        return 0;
      }
      long allowedTimeout = Math.min(millisToSatisfyMinPermits + this.baseTimeout, timeoutMillis);

      while (requestedPermits > minPermits) {
        if (this.tokenBucket.getTokens(requestedPermits, allowedTimeout, TimeUnit.MILLISECONDS)) {
          return requestedPermits;
        }
        requestedPermits /= 2;
      }

      if (this.tokenBucket.getTokens(minPermits, allowedTimeout, TimeUnit.MILLISECONDS)) {
        return minPermits;
      }
    } catch (InterruptedException ie) {
      // Fallback to returning 0
    }

    return 0;
  }

}
