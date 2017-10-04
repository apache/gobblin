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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

import parquet.example.data.Group;
import parquet.hadoop.ParquetWriter;


/**
 * An extension to {@link FsDataWriter} that writes in Parquet format in the form of {@link Group}s.
 *
 * <p>
 *   This implementation allows users to specify the {@link parquet.hadoop.CodecFactory} to use through the configuration
 *   property {@link ConfigurationKeys#WRITER_CODEC_TYPE}. By default, the deflate codec is used.
 * </p>
 *
 * @author tilakpatidar
 */
public class ParquetHdfsDataWriter extends FsDataWriter<Group> {
  private final ParquetWriter<Group> writer;
  protected final AtomicLong count = new AtomicLong(0);

  public ParquetHdfsDataWriter(ParquetDataWriterBuilder builder, State state)
      throws IOException {
    super(builder, state);
    this.writer = builder.getWriter((int) this.blockSize, this.stagingFile);
  }

  @Override
  public void write(Group record)
      throws IOException {
    this.writer.write(record);
    this.count.incrementAndGet();
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.writer.close();
    } finally {
      super.close();
    }
  }
}
