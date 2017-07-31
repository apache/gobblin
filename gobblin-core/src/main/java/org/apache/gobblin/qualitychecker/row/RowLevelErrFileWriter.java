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

package org.apache.gobblin.qualitychecker.row;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * A writer to store records that don't pass
 * the a row level policy check
 *
 * @author stakiar
 */
public class RowLevelErrFileWriter implements Closeable {
  private final FileSystem fs;
  private final Closer closer = Closer.create();
  private BufferedWriter writer;

  public RowLevelErrFileWriter(FileSystem fs) {
    this.fs = fs;
  }

  /**
   * Open a BufferedWriter
   * @param errFilePath path to write the file
   */
  public void open(Path errFilePath) throws IOException {
    this.fs.mkdirs(errFilePath.getParent());
    OutputStream os =
        this.closer.register(this.fs.exists(errFilePath) ? this.fs.append(errFilePath) : this.fs.create(errFilePath));
    this.writer = this.closer
        .register(new BufferedWriter(new OutputStreamWriter(os, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
  }

  /**
   * Write the string representation of the record
   * @param record the record to write
   */
  public void write(Object record) throws IOException {
    this.writer.write(record.toString());
  }

  /**
   * Close the writer
   */
  @Override
  public void close() throws IOException {
    this.closer.close();
  }
}
