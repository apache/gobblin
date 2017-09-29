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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_CODEC_TYPE;
import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_PREFIX;
import static org.apache.gobblin.writer.ParquetHdfsDataWriter.DEFAULT_PARQUET_WRITER;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;


public class ParquetDataWriterBuilder extends FsDataWriterBuilder<MessageType, Group> {
  public static final String WRITER_PARQUET_PAGE_SIZE = WRITER_PREFIX + ".parquet.page.size";
  public static final String WRITER_PARQUET_DICTIONARY_PAGE_SIZE = WRITER_PREFIX + ".parquet.dictionary.page.size";
  public static final String WRITER_PARQUET_DICTIONARY = WRITER_PREFIX + ".parquet.dictionary";
  public static final String WRITER_PARQUET_VALIDATE = WRITER_PREFIX + ".parquet.validate";
  public static final String WRITER_PARQUET_VERSION = WRITER_PREFIX + ".parquet.version";

  @Override
  public DataWriter<Group> build()
      throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

    switch (this.destination.getType()) {
      case HDFS:
        return new ParquetHdfsDataWriter(this, this.destination.getProperties());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }

  public ParquetWriter<Group> getWriter(long blockSize, Path stagingFile)
      throws IOException {
    State state = this.destination.getProperties();
    long pageSize = state.getPropAsLong(getProperty(WRITER_PARQUET_PAGE_SIZE), DEFAULT_PAGE_SIZE);
    long dictPageSize = state.getPropAsLong(getProperty(WRITER_PARQUET_DICTIONARY_PAGE_SIZE), DEFAULT_BLOCK_SIZE);
    boolean enableDictionary =
        state.getPropAsBoolean(getProperty(WRITER_PARQUET_DICTIONARY), DEFAULT_IS_DICTIONARY_ENABLED);
    boolean validate = state.getPropAsBoolean(getProperty(WRITER_PARQUET_VALIDATE), DEFAULT_IS_VALIDATING_ENABLED);
    CompressionCodecName codec = getCodecFromConfig();
    GroupWriteSupport support = new GroupWriteSupport();
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(this.schema, conf);
    ParquetProperties.WriterVersion writerVersion = getWriterVersion();
    return new ParquetWriter<>(stagingFile, support, codec, (int) blockSize, (int) pageSize, (int) dictPageSize,
        enableDictionary, validate, writerVersion, conf);
  }

  private ParquetProperties.WriterVersion getWriterVersion() {
    return ParquetProperties.WriterVersion.fromString(
        this.destination.getProperties().getProp(getProperty(WRITER_PARQUET_VERSION), DEFAULT_PARQUET_WRITER));
  }

  private CompressionCodecName getCodecFromConfig() {
    State state = this.destination.getProperties();
    String codecValue = Optional.ofNullable(state.getProp(getProperty(WRITER_CODEC_TYPE)))
        .orElse(CompressionCodecName.SNAPPY.toString());
    return CompressionCodecName.valueOf(codecValue.toUpperCase());
  }

  private String getProperty(String key) {
    return ForkOperatorUtils.getPropertyNameForBranch(key, this.getBranches(), this.getBranch());
  }
}
