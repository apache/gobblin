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

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Preconditions;

import org.apache.gobblin.configuration.State;


/**
 * An extension to {@link FsDataWriter} that writes {@link Writable} records using an
 * {@link org.apache.hadoop.mapred.OutputFormat} that implements {@link HiveOutputFormat}.
 *
 * The records are written using a {@link RecordWriter} created by
 * {@link HiveOutputFormat#getHiveRecordWriter(JobConf, org.apache.hadoop.fs.Path, Class, boolean,
 * java.util.Properties, org.apache.hadoop.util.Progressable)}.
 *
 * @author Ziyang Liu
 */
public class HiveWritableHdfsDataWriter extends FsDataWriter<Writable> {

  protected RecordWriter writer;
  protected final AtomicLong count = new AtomicLong(0);
  // the close method may be invoked multiple times, but the underlying writer only supports close being called once
  private boolean closed = false;

  public HiveWritableHdfsDataWriter(HiveWritableHdfsDataWriterBuilder<?> builder, State properties) throws IOException {
    super(builder, properties);

    Preconditions.checkArgument(this.properties.contains(HiveWritableHdfsDataWriterBuilder.WRITER_OUTPUT_FORMAT_CLASS));
    this.writer = getWriter();
  }

  private RecordWriter getWriter() throws IOException {
    try {
      HiveOutputFormat<?, ?> outputFormat = HiveOutputFormat.class
          .cast(Class.forName(this.properties.getProp(HiveWritableHdfsDataWriterBuilder.WRITER_OUTPUT_FORMAT_CLASS))
              .newInstance());

      @SuppressWarnings("unchecked")
      Class<? extends Writable> writableClass = (Class<? extends Writable>) Class
          .forName(this.properties.getProp(HiveWritableHdfsDataWriterBuilder.WRITER_WRITABLE_CLASS));

      // Merging Job Properties into JobConf for easy tuning
      JobConf loadedJobConf = new JobConf();
      for (Object key : this.properties.getProperties().keySet()) {
        loadedJobConf.set((String)key, this.properties.getProp((String)key));
      }

      return outputFormat.getHiveRecordWriter(loadedJobConf, this.stagingFile, writableClass, true,
          this.properties.getProperties(), null);
    } catch (Throwable t) {
      throw new IOException(String.format("Failed to create writer"), t);
    }
  }

  @Override
  public void write(Writable record) throws IOException {
    Preconditions.checkNotNull(record);

    this.writer.write(record);
    this.count.incrementAndGet();
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    if (!this.fs.exists(this.outputFile)) {
      return 0;
    }

    return this.fs.getFileStatus(this.outputFile).getLen();
  }

  @Override
  public void close() throws IOException {
    closeInternal();
    super.close();
  }

  @Override
  public void commit() throws IOException {
    closeInternal();
    super.commit();
  }

  private void closeInternal() throws IOException {
    // close the underlying writer if not already closed. The close can only be called once for the underlying writer,
    // so remember the state
    if (!this.closed) {
      this.writer.close(false);
      // release reference to allow GC since this writer can hold onto large buffers for some formats like ORC.
      this.writer = null;
      this.closed = true;
    }
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == HiveWritableHdfsDataWriter.class;
  }
}
