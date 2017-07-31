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
package org.apache.gobblin.test.crypto;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.gobblin.codec.StreamCodec;


/**
 * Simple encryption algorithm that just increments every byte sent
 * through it by 1. Useful for unit tests or proof of concept, but is not actually secure.
 */
public class InsecureShiftCodec implements StreamCodec {
  public static final String TAG = "insecure_shift";

  public InsecureShiftCodec(Map<String, Object> parameters) {
    // InsecureShiftCodec doesn't care about parameters
  }

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream) {
    return new FilterOutputStream(origStream) {
      @Override
      public void write(int b) throws IOException {
        out.write((b + 1) % 256);
      }

      @Override
      public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        for (int i = off; i < off + len; i++) {
          this.write(b[i]);
        }
      }

      @Override
      public void close() throws IOException {
        out.close();
      }
    };
  }

  @Override
  public InputStream decodeInputStream(InputStream in) {
    return new FilterInputStream(in) {
      @Override
      public int read() throws IOException {
        int upstream = in.read();
        if (upstream == 0) {
          upstream = 255;
        } else if (upstream > 0) {
          upstream--;
        }

        return upstream;
      }

      @Override
      public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++) {
          int result = read();
          if (result == -1) {
            return (i == 0) ? -1 : i;
          }

          b[off + i] = (byte) result;
        }

        return len;
      }
    };
  }

  @Override
  public String getTag() {
    return TAG;
  }
}
