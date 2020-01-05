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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.schema.MessageType;

import com.google.protobuf.Message;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.parquet.writer.AbstractParquetDataWriterBuilder;
import org.apache.gobblin.parquet.writer.ParquetWriterConfiguration;
import org.apache.gobblin.parquet.writer.ParquetWriterShim;


@Slf4j
public class ParquetDataWriterBuilder<S,D> extends AbstractParquetDataWriterBuilder<S,D> {

  /**
   * Build a version-specific {@link ParquetWriter} for given {@link ParquetWriterConfiguration}
   * @param writerConfiguration
   * @return
   * @throws IOException
   */
  @Override
  public ParquetWriterShim getVersionSpecificWriter(ParquetWriterConfiguration writerConfiguration)
      throws IOException {

    CompressionCodecName codecName = CompressionCodecName.fromConf(writerConfiguration.getCodecName());
    ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion
        .fromString(writerConfiguration.getWriterVersion());

    Configuration conf = new Configuration();
    ParquetWriter versionSpecificWriter = null;
    switch (writerConfiguration.getRecordFormat()) {
      case GROUP: {
        GroupWriteSupport.setSchema((MessageType) this.schema, conf);
        WriteSupport support = new GroupWriteSupport();
        versionSpecificWriter = new ParquetWriter<Group>(
            writerConfiguration.getAbsoluteStagingFile(),
            support,
            codecName,
            writerConfiguration.getBlockSize(),
            writerConfiguration.getPageSize(),
            writerConfiguration.getDictPageSize(),
            writerConfiguration.isDictionaryEnabled(),
            writerConfiguration.isValidate(),
            writerVersion,
            conf);
        break;
      }
      case AVRO:  {
        versionSpecificWriter = new AvroParquetWriter(
            writerConfiguration.getAbsoluteStagingFile(),
            (Schema) this.schema,
            codecName,
            writerConfiguration.getBlockSize(),
            writerConfiguration.getPageSize(),
            writerConfiguration.isDictionaryEnabled(),
            conf);
        break;
      }
      case PROTOBUF: {
        versionSpecificWriter = new ProtoParquetWriter(
            writerConfiguration.getAbsoluteStagingFile(),
            (Class<? extends Message>) this.schema,
            codecName,
            writerConfiguration.getBlockSize(),
            writerConfiguration.getPageSize(),
            writerConfiguration.isDictionaryEnabled(),
            writerConfiguration.isValidate());
        break;
      }
      default: throw new RuntimeException("Record format not supported");
    }
    ParquetWriter finalVersionSpecificWriter = versionSpecificWriter;

    return new ParquetWriterShim() {
      @Override
      public void write(Object record)
          throws IOException {
        finalVersionSpecificWriter.write(record);
      }

      @Override
      public void close()
          throws IOException {
        finalVersionSpecificWriter.close();
      }
    };
  }
}
