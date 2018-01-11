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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.gobblin.util.filesystem.ThrottledFileSystem;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.RateBasedLimiter;


/**
 * Subclass of {@link org.apache.hadoop.fs.FileSystem} that wraps with a {@link org.apache.gobblin.util.limiter.Limiter}
 * to control HDFS call rate.
 *
 *  <p>
 *  This classes uses Guava's {@link Cache} for storing {@link org.apache.hadoop.fs.FileSystem} URI to
 *  {@link org.apache.gobblin.util.limiter.Limiter} mapping.
 *  </p>
 *
 *  <p>
 *  For methods that require HDFS calls, this class will first acquire a permit using {@link org.apache.gobblin.util.limiter.Limiter},
 *  to make sure HDFS call rate is allowed by the uppper limit.
 *  </p>
 *
 *  @deprecated use {@link org.apache.gobblin.util.filesystem.ThrottledFileSystem}
 */
@Deprecated
public class RateControlledFileSystem extends ThrottledFileSystem {

  private static final int DEFAULT_MAX_CACHE_SIZE = 100;
  private static final Cache<String, RateBasedLimiter> FS_URI_TO_RATE_LIMITER_CACHE =
      CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE).build();

  private final long limitPerSecond;
  private final Callable<RateBasedLimiter> callableLimiter;

  /**
   * Determines whether the file system is rate controlled, and if so, returns the allowed rate in operations per
   * second.
   * @param fs {@link FileSystem} to check for rate control.
   * @return {@link Optional#absent} if file system is not rate controlled, otherwise, the rate in operations per second.
   */
  public static Optional<Long> getRateIfRateControlled(FileSystem fs) {
    if (fs instanceof Decorator) {
      List<Object> lineage = DecoratorUtils.getDecoratorLineage(fs);
      for (Object obj : lineage) {
        if (obj instanceof RateControlledFileSystem) {
          return Optional.of(((RateControlledFileSystem) obj).limitPerSecond);
        }
      }
      return Optional.absent();
    }
    return Optional.absent();
  }

  public RateControlledFileSystem(FileSystem fs, final long limitPerSecond) {
    super(fs, null, null);
    this.limitPerSecond = limitPerSecond;
    this.callableLimiter = new Callable<RateBasedLimiter>() {
      @Override
      public RateBasedLimiter call() throws Exception {
        return new RateBasedLimiter(limitPerSecond);
      }
    };
  }

  public void startRateControl() throws ExecutionException {
    getRateLimiter().start();
  }

  protected Limiter getRateLimiter() {
    try {

      String key = getUri().toString();
      RateBasedLimiter limiter = FS_URI_TO_RATE_LIMITER_CACHE.get(key, this.callableLimiter);
      if (limiter.getRateLimitPerSecond() < this.limitPerSecond) {
        try {
          limiter = this.callableLimiter.call();
          FS_URI_TO_RATE_LIMITER_CACHE.put(key, limiter);
        } catch (Exception exc) {
          throw new ExecutionException(exc);
        }
      }

      return limiter;
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }
}
