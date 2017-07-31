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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;


/**
 * Class that wraps an {@link InputStream} to support {@link Seekable} and {@link PositionedReadable}
 */
public class SeekableFSInputStream extends FSInputStream {

  private InputStream in;
  private long pos;

  public SeekableFSInputStream(InputStream in) {
    this.in = in;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int val = this.in.read(b, off, len);
    if (val > 0) {
      this.pos += val;
    }
    return val;
  }

  @Override
  public long getPos() throws IOException {
    return this.pos;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.pos += this.in.skip(pos - this.pos);
  }

  @Override
  public boolean seekToNewSource(long arg0) throws IOException {
    return false;
  }

  @Override
  public int read() throws IOException {
    int val = this.in.read();
    if (val > 0) {
      this.pos += val;
    }
    return val;
  }

  @Override
  public void close() throws IOException {
    super.close();
    in.close();
  }
}
