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

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ForkOperatorUtils;

import static org.apache.gobblin.configuration.ConfigurationKeys.LOCAL_FS_URI;
import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_CODEC_TYPE;
import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_FILE_SYSTEM_URI;
import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_PREFIX;


/**
 * Holds configuration for the {@link ParquetHdfsDataWriter}
 */
@Getter @ToString
public class ParquetWriterConfiguration {
  public static final String WRITER_PARQUET_PAGE_SIZE = WRITER_PREFIX + ".parquet.pageSize";
  public static final String WRITER_PARQUET_DICTIONARY_PAGE_SIZE = WRITER_PREFIX + ".parquet.dictionaryPageSize";
  public static final String WRITER_PARQUET_DICTIONARY = WRITER_PREFIX + ".parquet.dictionary";
  public static final String WRITER_PARQUET_VALIDATE = WRITER_PREFIX + ".parquet.validate";
  public static final String WRITER_PARQUET_VERSION = WRITER_PREFIX + ".parquet.version";
  public static final String DEFAULT_PARQUET_WRITER = "v1";
  public static final String WRITER_PARQUET_FORMAT = WRITER_PREFIX + ".parquet.format";
  public static final String DEFAULT_PARQUET_FORMAT = "group";



  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
  public static final String DEFAULT_COMPRESSION_CODEC_NAME = "UNCOMPRESSED";
  public static final String[] ALLOWED_COMPRESSION_CODECS = {"SNAPPY", "LZO", "UNCOMPRESSED", "GZIP"};
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
  public static final String DEFAULT_WRITER_VERSION = "v1";
  public static final String[] ALLOWED_WRITER_VERSIONS = {"v1", "v2"};


  private final int pageSize;
  private final int dictPageSize;
  private final boolean dictionaryEnabled;
  private final boolean validate;
  private final String writerVersion;
  private final ParquetRecordFormat recordFormat;


  private final int numBranches;
  private final int branchId;
  private final String codecName;
  private final Path absoluteStagingFile;
  private final int blockSize;

  public ParquetWriterConfiguration(State state, int numBranches, int branchId, Path stagingFile, int blockSize) {
    this(ConfigUtils.propertiesToConfig(state.getProperties()), numBranches, branchId, stagingFile, blockSize);
  }


  private String getProperty(String key) {
    return ForkOperatorUtils.getPropertyNameForBranch(key, numBranches, branchId);
  }

  public static ParquetRecordFormat getRecordFormatFromConfig(Config config) {
    String writeSupport = ConfigUtils.getString(config, WRITER_PARQUET_FORMAT, DEFAULT_PARQUET_FORMAT);
    ParquetRecordFormat recordFormat = ParquetRecordFormat.valueOf(writeSupport.toUpperCase());
    return recordFormat;
  }


  ParquetWriterConfiguration(Config config, int numBranches, int branchId, Path stagingFile, int blockSize) {
    this.numBranches = numBranches;
    this.branchId = branchId;
    this.pageSize = ConfigUtils.getInt(config, getProperty(WRITER_PARQUET_PAGE_SIZE), DEFAULT_PAGE_SIZE);
    this.dictPageSize = ConfigUtils.getInt(config, getProperty(WRITER_PARQUET_DICTIONARY_PAGE_SIZE), DEFAULT_BLOCK_SIZE);
    this.dictionaryEnabled =
        ConfigUtils.getBoolean(config, getProperty(WRITER_PARQUET_DICTIONARY), DEFAULT_IS_DICTIONARY_ENABLED);
    this.validate = ConfigUtils.getBoolean(config, getProperty(WRITER_PARQUET_VALIDATE), DEFAULT_IS_VALIDATING_ENABLED);
    String rootURI = ConfigUtils.getString(config, WRITER_FILE_SYSTEM_URI, LOCAL_FS_URI);
    this.absoluteStagingFile = new Path(rootURI, stagingFile);
    this.codecName = ConfigUtils.getString(config,getProperty(WRITER_CODEC_TYPE), DEFAULT_COMPRESSION_CODEC_NAME);
    this.recordFormat = getRecordFormatFromConfig(config);
    this.writerVersion = ConfigUtils.getString(config, getProperty(WRITER_PARQUET_VERSION), DEFAULT_WRITER_VERSION);
    this.blockSize = blockSize;
  }
}
