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
package org.apache.gobblin.parquet.writer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.FsDataWriter;


/**
 * An extension to {@link FsDataWriter} that writes in Parquet formats.
 *
 * <p>
 *   This implementation allows users to specify different formats and codecs
 *   through {@link ParquetWriterConfiguration} to write data.
 * </p>
 *
 * @author tilakpatidar
 */
public class ParquetHdfsDataWriter<D> extends FsDataWriter<D> {
  private final ParquetWriterShim writer;
  protected final AtomicLong count = new AtomicLong(0);

  public ParquetHdfsDataWriter(AbstractParquetDataWriterBuilder builder, State state)
      throws IOException {
    super(builder, state);
    this.writer = builder.getWriter((int) this.blockSize, this.stagingFile);
  }

  @Override
  public void write(D record)
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
