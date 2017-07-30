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

package gobblin.source.extractor.utils;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * This class provides similar function as {@link org.apache.avro.mapred.FsInput}.
 * The difference is that it allows extractor to use customized {@link org.apache.hadoop.fs.FileSystem},
 * especially, when file system proxy is enabled.
 *
 */
public class ProxyFsInput implements Closeable, SeekableInput {
  private final FSDataInputStream stream;
  private final long len;

  public ProxyFsInput(Path path, FileSystem fs) throws IOException {
    this.len = fs.getFileStatus(path).getLen();
    this.stream = fs.open(path);
  }

  @Override
  public long length() {
    return this.len;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return this.stream.read(b, off, len);
  }

  @Override
  public void seek(long p) throws IOException {
    this.stream.seek(p);
  }

  @Override
  public long tell() throws IOException {
    return this.stream.getPos();
  }

  @Override
  public void close() throws IOException {
    this.stream.close();
  }
}
