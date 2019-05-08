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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.example.data.Group;


/**
 * An extension to {@link MultipleFilesFsDataWriter} that writes in Parquet format in the form of {@link Group}s.
 * Using {@link MultipleFilesFsDataWriter} allows the user to control the number of {@link Group} records per file.
 * Use property {@link ConfigurationKeys#WRITER_RECORDS_PER_FILE_THRESHOLD} to set number of records per file.
 * <p>
 *   This implementation allows users to specify the {@link parquet.hadoop.CodecFactory} to use through the configuration
 *   property {@link ConfigurationKeys#WRITER_CODEC_TYPE}. By default, the deflate codec is used.
 * </p>
 *
 * @author tilakpatidar
 */
public class ParquetMultipleFilesHdfsDataWriter extends MultipleFilesFsDataWriter<Group> {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMultipleFilesHdfsDataWriter.class);

  public ParquetMultipleFilesHdfsDataWriter(MultipleFilesFsDataWriterBuilder<?, Group> builder, State state)
      throws IOException {
    super(builder, state);
  }

  @Override
  public void writeWithCurrentWriter(FileFormatWriter<Group> writer, Group record)
      throws IOException {
    writer.write(record);
  }

  @Override
  public void closeCurrentWriter(FileFormatWriter<Group> writer)
      throws IOException {
    try {
      writer.close();
    } finally {
      super.close();
    }
    LOG.info("Current writer closed");
  }
}
