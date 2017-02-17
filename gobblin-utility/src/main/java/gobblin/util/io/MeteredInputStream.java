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

package gobblin.util.io;

import java.io.FilterInputStream;
import java.io.FilterStreamUnpacker;
import java.io.IOException;
import java.io.InputStream;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import lombok.Getter;


public class MeteredInputStream extends FilterInputStream {

  public static Optional<MeteredInputStream> findWrappedMeteredInputStream(InputStream is) {
    if (is instanceof FilterInputStream) {
      Optional<MeteredInputStream> meteredInputStream =
          findWrappedMeteredInputStream(FilterStreamUnpacker.unpackFilterInputStream((FilterInputStream) is));
      if (meteredInputStream.isPresent()) {
        return meteredInputStream;
      }
    }
    if (is instanceof MeteredInputStream) {
      return Optional.of((MeteredInputStream) is);
    }
    return Optional.absent();
  }

  @Getter
  BatchedMeterDecorator meter;

  public MeteredInputStream(InputStream in) {
    this(in, null);
  }

  public MeteredInputStream(InputStream in, Meter meter) {
    super(in);
    this.meter = new BatchedMeterDecorator(meter == null ? new Meter() : meter, 1000);
  }

  @Override
  public int read() throws IOException {
    this.meter.mark();
    return super.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readBytes = super.read(b, off, len);
    this.meter.mark(readBytes);
    return readBytes;
  }
}
