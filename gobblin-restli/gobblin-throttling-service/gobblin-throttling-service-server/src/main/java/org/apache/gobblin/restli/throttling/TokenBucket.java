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

import com.google.common.base.Preconditions;

import lombok.AccessLevel;
import lombok.Getter;


/**
 * An implementation of Token Bucket (https://en.wikipedia.org/wiki/Token_bucket).
 *
 * This class is intended to limit the rate at which tokens are used to a given QPS. It can store tokens for future usage.
 */
public class TokenBucket {

  @Getter(AccessLevel.PROTECTED)
  private double tokensPerMilli;
  private double maxBucketSizeInTokens;

  private volatile long nextTokenAvailableMillis;
  private volatile double tokensStored;

  public TokenBucket(long qps, long maxBucketSizeInMillis) {
    this.nextTokenAvailableMillis = System.currentTimeMillis();
    resetQPS(qps, maxBucketSizeInMillis);
  }

  public void resetQPS(long qps, long maxBucketSizeInMillis) {
    Preconditions.checkArgument(qps > 0, "QPS must be positive.");
    Preconditions.checkArgument(maxBucketSizeInMillis >= 0, "Max bucket size must be non-negative.");

    long now = System.currentTimeMillis();
    synchronized (this) {
      updateTokensStored(now);
      if (this.nextTokenAvailableMillis > now) {
        this.tokensStored -= (this.nextTokenAvailableMillis - now) * this.tokensPerMilli;
      }
      this.tokensPerMilli = (double) qps / 1000;
      this.maxBucketSizeInTokens = this.tokensPerMilli * maxBucketSizeInMillis;
    }
  }

  /**
   * Attempt to get the specified amount of tokens within the specified timeout. If the tokens cannot be retrieved in the
   * specified timeout, the call will return false immediately, otherwise, the call will block until the tokens are available.
   *
   * @return true if the tokens are granted.
   * @throws InterruptedException
   */
  public boolean getTokens(long tokens, long timeout, TimeUnit timeoutUnit) throws InterruptedException {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    long wait;
    synchronized (this) {
      wait = tryReserveTokens(tokens, timeoutMillis);
    }

    if (wait < 0) {
      return false;
    }
    if (wait == 0) {
      return true;
    }

    Thread.sleep(wait);
    return true;
  }

  /**
   * Get the current number of stored tokens. Note this is a snapshot of the object, and there is no guarantee that those
   * tokens will be available at any point in the future.
   */
  public long getStoredTokens() {
    synchronized (this) {
      updateTokensStored(System.currentTimeMillis());
    }
    return (long) this.tokensStored;
  }

  /**
   * Note: this method should only be called while holding the class lock. For performance, the lock is not explicitly
   * acquired.
   *
   * @return the wait until the tokens are available or negative if they can't be acquired in the give timeout.
   */
  private long tryReserveTokens(long tokens, long maxWaitMillis) {
    long now = System.currentTimeMillis();
    long waitUntilNextTokenAvailable = Math.max(0, this.nextTokenAvailableMillis - now);

    updateTokensStored(now);
    if (tokens <= this.tokensStored) {
      this.tokensStored -= tokens;
      return waitUntilNextTokenAvailable;
    }

    double additionalNeededTokens = tokens - this.tokensStored;
    // casting to long will round towards 0
    long additionalWaitForEnoughTokens = (long) (additionalNeededTokens / this.tokensPerMilli) + 1;
    long totalWait = waitUntilNextTokenAvailable + additionalWaitForEnoughTokens;
    if (totalWait > maxWaitMillis) {
      return -1;
    }
    this.tokensStored = this.tokensPerMilli * additionalWaitForEnoughTokens - additionalNeededTokens;
    this.nextTokenAvailableMillis = this.nextTokenAvailableMillis + additionalWaitForEnoughTokens;
    return totalWait;
  }

  /**
   * Note: this method should only be called while holding the class lock. For performance, the lock is not explicitly
   * acquired.
   */
  private void updateTokensStored(long now) {
    if (now <= this.nextTokenAvailableMillis) {
      return;
    }
    long millisUnaccounted = now - this.nextTokenAvailableMillis;
    double newTokens = millisUnaccounted * this.tokensPerMilli;
    this.nextTokenAvailableMillis = now;
    this.tokensStored = Math.min(this.tokensStored + newTokens, Math.max(this.tokensStored, this.maxBucketSizeInTokens));
  }

}
