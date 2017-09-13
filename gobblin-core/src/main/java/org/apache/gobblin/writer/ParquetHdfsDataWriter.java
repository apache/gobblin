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

import static parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;


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
  private final long dictionaryPageSize;
  private final boolean enableDictionary;
  private final boolean validate;
  private final ParquetProperties.WriterVersion writerVersion;

  public ParquetHdfsDataWriter(FsDataWriterBuilder<MessageType, Group> builder, State properties)
      throws IOException {
    super(builder, properties);
    schema = builder.getSchema();
    GroupWriteSupport support = new GroupWriteSupport();
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    String codecName = Optional.ofNullable(this.properties.getProp(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_CODEC_TYPE, this.numBranches, this.branchId)))
        .orElse(CompressionCodecName.SNAPPY.toString());
    CompressionCodecName codec = CompressionCodecName.valueOf(codecName.toUpperCase());
    this.pageSize = properties.getPropAsLong(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARQUET_PAGE_SIZE, this.numBranches, this.branchId),
        ParquetWriter.DEFAULT_PAGE_SIZE);
    this.dictionaryPageSize = properties.getPropAsLong(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARQUET_DICTIONARY_PAGE_SIZE, this.numBranches,
            this.branchId), ParquetWriter.DEFAULT_BLOCK_SIZE);
    this.enableDictionary = properties.getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARQUET_DICTIONARY, this.numBranches, this.branchId),
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED);
    this.validate = properties.getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARQUET_VALIDATE, this.numBranches, this.branchId),
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);
    this.writerVersion = ParquetProperties.WriterVersion.fromString(properties.getProp(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARQUET_VERSION, this.numBranches, this.branchId),
        PARQUET_1_0.toString()));
    this.writer = new ParquetWriter<>(this.stagingFile, support, codec, (int) this.blockSize, (int) this.pageSize,
        (int) this.dictionaryPageSize, this.enableDictionary, this.validate, PARQUET_1_0, conf);
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
}
