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
package org.apache.gobblin.util.test;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;

/**
 * A simple writer implementation that writes the output to Stdout
 */
public class StdoutWriter<D> implements DataWriter<D> {
  private long _numRecordsWritten = 0;
  private long _numBytesWritten = 0;

  @Override
  public void close() {
    // NO-OP
  }

  @Override
  public void write(D record) throws IOException {
    if (null != record) {
      String s = record.toString();
      System.out.println(s);
      ++ _numRecordsWritten;
      _numBytesWritten += s.getBytes(Charset.defaultCharset()).length;
    }
  }

  @Override
  public void commit() {
    // NO-OP
  }

  @Override
  public void cleanup() {
    // NO-OP
  }

  @Override
  public long recordsWritten() {
    return _numRecordsWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    return _numBytesWritten;
  }

  @Override
  public void flush() throws IOException {
    System.out.flush();
  }

  public static class Builder<D> extends DataWriterBuilder<Object, D> {
    @Override
    public DataWriter<D> build() throws IOException {
      return new StdoutWriter<>();
    }

  }

}
