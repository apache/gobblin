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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import parquet.example.data.Group;
import parquet.hadoop.ParquetWriter;
import parquet.schema.MessageType;

import static org.apache.gobblin.writer.ParquetDataWriterBuilder.buildParquetWriter;


public class ParquetMultipleFilesDataWriterBuilder extends MultipleFilesFsDataWriterBuilder<MessageType, Group> {

  @Override
  public DataWriter<Group> build()
      throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

    switch (this.destination.getType()) {
      case HDFS:
        return new ParquetMultipleFilesHdfsDataWriter(this, this.destination.getProperties());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }

  /**
   * Build a {@link ParquetWriter<Group>} for given file path with a block size.
   * @param blockSize
   * @param stagingFile
   * @return
   * @throws IOException
   */
  @Override
  public FileFormatWriter<Group> getNewWriter(int blockSize, Path stagingFile)
      throws IOException {
    ParquetWriter<Group> groupParquetWriter =
        buildParquetWriter(this.destination.getProperties(), this.schema, this.branches, this.branch, stagingFile,
            blockSize);

    return new FileFormatWriter<Group>() {

      @Override
      public void write(Group record)
          throws IOException {
        groupParquetWriter.write(record);
      }

      @Override
      public void close()
          throws IOException {
        groupParquetWriter.close();
      }
    };
  }
}
