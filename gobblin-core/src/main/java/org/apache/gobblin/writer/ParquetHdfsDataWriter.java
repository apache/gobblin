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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.hadoop.conf.Configuration;

import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;


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
  private final MessageType schema;
  private final ParquetWriter<Group> writer;
  protected final AtomicLong count = new AtomicLong(0);
  private final long pageSize;
  private final long dictPageSize;
  private final boolean enableDictionary;
  private final boolean validate;
  public static final String DEFAULT_PARQUET_WRITER = "v1";

  public ParquetHdfsDataWriter(FsDataWriterBuilder<MessageType, Group> builder, State state)
      throws IOException {
    super(builder, state);
    this.schema = builder.getSchema();
    this.pageSize = state.getPropAsLong(getProperty(WRITER_PARQUET_PAGE_SIZE), DEFAULT_PAGE_SIZE);
    this.dictPageSize = state.getPropAsLong(getProperty(WRITER_PARQUET_DICTIONARY_PAGE_SIZE), DEFAULT_BLOCK_SIZE);
    this.enableDictionary =
        state.getPropAsBoolean(getProperty(WRITER_PARQUET_DICTIONARY), DEFAULT_IS_DICTIONARY_ENABLED);
    this.validate = state.getPropAsBoolean(getProperty(WRITER_PARQUET_VALIDATE), DEFAULT_IS_VALIDATING_ENABLED);
    this.writer = buildParquetWriter();
  }

  @Override
  public void write(Group record)
      throws IOException {
    try {
      writer.write(record);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.count.incrementAndGet();
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }

  @Override
  public void close()
      throws IOException {
    this.writer.close();
    super.close();
  }

  private ParquetWriter<Group> buildParquetWriter()
      throws IOException {
    CompressionCodecName codec = getCodecFromConfig();
    GroupWriteSupport support = new GroupWriteSupport();
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(this.schema, conf);
    ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion
        .fromString(this.properties.getProp(getProperty(WRITER_PARQUET_VERSION), DEFAULT_PARQUET_WRITER));
    return new ParquetWriter<>(this.stagingFile, support, codec, (int) this.blockSize, (int) this.pageSize,
        (int) this.dictPageSize, this.enableDictionary, this.validate, writerVersion, conf);
  }

  private CompressionCodecName getCodecFromConfig() {
    String codecValue = Optional.ofNullable(this.properties.getProp(getProperty(WRITER_CODEC_TYPE)))
        .orElse(CompressionCodecName.SNAPPY.toString());
    return CompressionCodecName.valueOf(codecValue.toUpperCase());
  }

  private String getProperty(String key) {
    return ForkOperatorUtils.getPropertyNameForBranch(key, this.numBranches, this.branchId);
  }
}
