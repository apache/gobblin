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

package org.apache.gobblin.util.io;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.gobblin.util.limiter.Limiter;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * A throttled {@link InputStream}.
 */
@NotThreadSafe
public class ThrottledInputStream extends FilterInputStream {

  private final Limiter limiter;
  private final MeteredInputStream meter;

  private long prevCount;

  /**
   * Builds a {@link ThrottledInputStream}.
   *
   * It is recommended to use a {@link StreamThrottler} for creation of {@link ThrottledInputStream}s.
   *
   * @param in {@link InputStream} to throttle.
   * @param limiter {@link Limiter} to use for throttling.
   * @param meter {@link MeteredInputStream} used to measure the {@link InputStream}. Note the {@link MeteredInputStream}
   *                                        MUST be in the {@link FilterInputStream} chain of {@link #in}.
   */
  public ThrottledInputStream(InputStream in, Limiter limiter, MeteredInputStream meter) {
    super(in);
    this.limiter = limiter;
    this.meter = meter;
    // In case the meter was already used
    this.prevCount = this.meter.getBytesProcessedMeter().getCount();
  }

  @Override
  public int read() throws IOException {
    blockUntilPermitsAvailable();
    return this.in.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    blockUntilPermitsAvailable();
    return this.in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    blockUntilPermitsAvailable();
    return this.in.read(b, off, len);
  }

  @Override
  public synchronized void reset() throws IOException {
    super.reset();
    this.prevCount = this.meter.getBytesProcessedMeter().getCount();
  }

  private void blockUntilPermitsAvailable() {
    try {
      long currentCount = this.meter.getBytesProcessedMeter().getCount();
      long permitsNeeded = currentCount - this.prevCount;
      this.prevCount = currentCount;
      Closeable permit = this.limiter.acquirePermits(permitsNeeded);
      if (permit == null) {
        throw new RuntimeException("Could not acquire permits.");
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
