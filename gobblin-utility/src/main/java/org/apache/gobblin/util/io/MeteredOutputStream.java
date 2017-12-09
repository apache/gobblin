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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link FilterOutputStream} that counts the bytes read from the underlying {@link OutputStream}.
 */
@Slf4j
public class MeteredOutputStream extends FilterOutputStream implements MeteredStream {

  /**
   * Find the lowest {@link MeteredOutputStream} in a chain of {@link FilterOutputStream}s.
   */
  public static Optional<MeteredOutputStream> findWrappedMeteredOutputStream(OutputStream os) {
    if (os instanceof FilterOutputStream) {
      try {
        Optional<MeteredOutputStream> meteredOutputStream =
            findWrappedMeteredOutputStream(FilterStreamUnpacker.unpackFilterOutputStream((FilterOutputStream) os));
        if (meteredOutputStream.isPresent()) {
          return meteredOutputStream;
        }
      } catch (IllegalAccessException iae) {
        log.warn("Cannot unpack input stream due to SecurityManager.", iae);
        // Do nothing, we can't unpack the FilterInputStream due to security restrictions
      }
    }
    if (os instanceof MeteredOutputStream) {
      return Optional.of((MeteredOutputStream) os);
    }
    return Optional.absent();
  }

  BatchedMeterDecorator meter;

  /**
   * Builds a {@link MeteredOutputStream}.
   * @param out The {@link OutputStream} to measure.
   * @param meter A {@link Meter} to use for measuring the {@link OutputStream}. If null, a new {@link Meter} will be created.
   * @param updateFrequency For performance, {@link MeteredInputStream} will batch {@link Meter} updates to this many bytes.
   */
  @Builder
  public MeteredOutputStream(OutputStream out, Meter meter, int updateFrequency) {
    super(out);
    this.meter = new BatchedMeterDecorator(meter == null ? new Meter() : meter, updateFrequency > 0 ? updateFrequency : 1000);
  }

  @Override
  public void write(int b) throws IOException {
    this.meter.mark();
    this.out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    this.meter.mark(len);
    this.out.write(b, off, len);
  }

  @Override
  public Meter getBytesProcessedMeter() {
    return this.meter.getUnderlyingMeter();
  }
}
