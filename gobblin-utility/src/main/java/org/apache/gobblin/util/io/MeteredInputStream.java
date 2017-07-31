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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link FilterInputStream} that counts the bytes read from the underlying {@link InputStream}.
 */
@Slf4j
public class MeteredInputStream extends FilterInputStream implements MeteredStream {

  /**
   * Find the lowest {@link MeteredInputStream} in a chain of {@link FilterInputStream}s.
   */
  public static Optional<MeteredInputStream> findWrappedMeteredInputStream(InputStream is) {
    if (is instanceof FilterInputStream) {
      try {
        Optional<MeteredInputStream> meteredInputStream =
            findWrappedMeteredInputStream(FilterStreamUnpacker.unpackFilterInputStream((FilterInputStream) is));
        if (meteredInputStream.isPresent()) {
          return meteredInputStream;
        }
      } catch (IllegalAccessException iae) {
        log.warn("Cannot unpack input stream due to SecurityManager.", iae);
        // Do nothing, we can't unpack the FilterInputStream due to security restrictions
      }
    }
    if (is instanceof MeteredInputStream) {
      return Optional.of((MeteredInputStream) is);
    }
    return Optional.absent();
  }

  private BatchedMeterDecorator meter;

  /**
   * Builds a {@link MeteredInputStream}.
   * @param in The {@link InputStream} to measure.
   * @param meter A {@link Meter} to use for measuring the {@link InputStream}. If null, a new {@link Meter} will be created.
   * @param updateFrequency For performance, {@link MeteredInputStream} will batch {@link Meter} updates to this many bytes.
   */
  @Builder
  private MeteredInputStream(InputStream in, Meter meter, int updateFrequency) {
    super(in);
    this.meter = new BatchedMeterDecorator(meter == null ? new Meter() : meter, updateFrequency > 0 ? updateFrequency : 1000);
  }

  @Override
  public int read() throws IOException {
    int bte = super.read();
    if (bte >= 0) {
      this.meter.mark();
    }
    return bte;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readBytes = super.read(b, off, len);
    this.meter.mark(readBytes);
    return readBytes;
  }

  @Override
  public Meter getBytesProcessedMeter() {
    return this.meter.getUnderlyingMeter();
  }
}
