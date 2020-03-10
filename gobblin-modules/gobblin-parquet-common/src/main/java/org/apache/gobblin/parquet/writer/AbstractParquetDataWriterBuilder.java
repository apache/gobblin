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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.FsDataWriterBuilder;
import org.apache.gobblin.writer.WriterOutputFormat;


@Slf4j
public abstract class AbstractParquetDataWriterBuilder<S,D> extends FsDataWriterBuilder<S, D> {

  @Override
  public DataWriter<D> build()
      throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

    switch (this.destination.getType()) {
      case HDFS:
        return new ParquetHdfsDataWriter<D>(this, this.destination.getProperties());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }

  protected abstract ParquetWriterShim getVersionSpecificWriter(ParquetWriterConfiguration writerConfiguration)
      throws IOException;

  /**
   * Build a {@link ParquetWriterShim <D>} for given file path with a block size.
   * @param blockSize
   * @param stagingFile
   * @return
   * @throws IOException
   */
  public ParquetWriterShim<D> getWriter(int blockSize, Path stagingFile)
      throws IOException {
    State state = this.destination.getProperties();
    ParquetWriterConfiguration writerConfiguration =
        new ParquetWriterConfiguration(state, this.getBranches(), this.getBranch(), stagingFile, blockSize);

    log.info("Parquet writer configured with {}", writerConfiguration);
    return getVersionSpecificWriter(writerConfiguration);
  }

}
