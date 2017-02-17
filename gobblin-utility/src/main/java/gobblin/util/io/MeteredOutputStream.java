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

import java.io.FilterOutputStream;
import java.io.FilterStreamUnpacker;
import java.io.IOException;
import java.io.OutputStream;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import lombok.Getter;


public class MeteredOutputStream extends FilterOutputStream {

  public static Optional<MeteredOutputStream> findWrappedMeteredOutputStream(OutputStream is) {
    if (is instanceof FilterOutputStream) {
      Optional<MeteredOutputStream> meteredOutputStream =
          findWrappedMeteredOutputStream(FilterStreamUnpacker.unpackFilterOutputStream((FilterOutputStream) is));
      if (meteredOutputStream.isPresent()) {
        return meteredOutputStream;
      }
    }
    if (is instanceof MeteredOutputStream) {
      return Optional.of((MeteredOutputStream) is);
    }
    return Optional.absent();
  }

  @Getter
  BatchedMeterDecorator meter;

  public MeteredOutputStream(OutputStream out) {
    this(out, null);
  }

  public MeteredOutputStream(OutputStream out, Meter meter) {
    super(out);
    this.meter = new BatchedMeterDecorator(meter == null ? new Meter() : meter, 1000);
  }

  @Override
  public void write(int b) throws IOException {
    this.meter.mark();
    out.write(b);
  }

}
