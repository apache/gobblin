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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;


public class MeteredInputStreamTest {

  @Test
  public void test() throws Exception {
    InputStream is = new ByteArrayInputStream("aabbccddee".getBytes(Charsets.UTF_8));

    Meter meter = new Meter();
    MeteredInputStream mis = MeteredInputStream.builder().in(is).meter(meter).updateFrequency(1).build();

    InputStream skipped = new MyInputStream(mis);
    DataInputStream dis = new DataInputStream(skipped);

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    IOUtils.copy(dis, os);
    String output = os.toString(Charsets.UTF_8.name());

    Assert.assertEquals(output, "abcde");

    Optional<MeteredInputStream> meteredOpt = MeteredInputStream.findWrappedMeteredInputStream(dis);
    Assert.assertEquals(meteredOpt.get(), mis);
    Assert.assertEquals(meteredOpt.get().getBytesProcessedMeter().getCount(), 10);
  }

  /**
   * An input stream that skips every second byte
   */
  public static class MyInputStream extends FilterInputStream {
    public MyInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      int bte = super.read();
      if (bte == -1) {
        return bte;
      }
      return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      for (int i = 0; i < len; i++) {
        byte bte = (byte) read();
        if (bte == -1) {
          return i == 0 ? -1 : i;
        }
        b[off + i] = bte;
      }
      return len;
    }
  }

}
