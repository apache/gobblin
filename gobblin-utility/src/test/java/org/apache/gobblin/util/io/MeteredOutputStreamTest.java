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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;


public class MeteredOutputStreamTest {

  @Test
  public void test() throws Exception {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    Meter meter = new Meter();
    MeteredOutputStream mos = MeteredOutputStream.builder().out(outputStream).meter(meter).updateFrequency(1).build();

    MyOutputStream duplicated = new MyOutputStream(mos);
    DataOutputStream dos = new DataOutputStream(duplicated);

    dos.write("abcde".getBytes(Charsets.UTF_8));

    Assert.assertEquals(outputStream.toString(Charsets.UTF_8.name()), "aabbccddee");
    Optional<MeteredOutputStream> meteredOutputStream = MeteredOutputStream.findWrappedMeteredOutputStream(dos);
    Assert.assertEquals(meteredOutputStream.get(), mos);
    Assert.assertEquals(meteredOutputStream.get().getBytesProcessedMeter().getCount(), 10);
  }

  /**
   * An {@link OutputStream} that duplicates every byte.
   */
  private static class MyOutputStream extends FilterOutputStream {
    public MyOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      this.out.write(b);
      this.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      for (int i = 0; i < len; i++) {
        write(b[off + i]);
      }
    }
  }

}
