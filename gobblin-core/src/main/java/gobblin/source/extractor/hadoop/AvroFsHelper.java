/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.Closer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.SizeAwareFileBasedHelper;
import gobblin.source.extractor.utils.ProxyFsInput;
import gobblin.util.HadoopUtils;

public class AvroFsHelper extends HadoopFsHelper implements SizeAwareFileBasedHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroFsHelper.class);

  public AvroFsHelper(State state) {
    this(state, HadoopUtils.newConfiguration());
  }

  public AvroFsHelper(State state, Configuration configuration) {
    super(state, configuration);
  }

  @Override
  public void close() throws FileBasedHelperException {

  }

  /**
   * Returns an {@link InputStream} to the specified file.
   * <p>
   * Note: It is the caller's responsibility to close the returned {@link InputStream}.
   * </p>
   * @param path The path to the file to open.
   * @return An {@link InputStream} for the specified file.
   * @throws FileBasedHelperException if there is a problem opening the {@link InputStream} for the specified file.
   */
  @Override
  public InputStream getFileStream(String path) throws FileBasedHelperException {
    try {
      Path p = new Path(path);
      InputStream in = this.getFileSystem().open(p);
      // Account for compressed files (e.g. gzip).
      // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/input/WholeTextFileRecordReader.scala
      CompressionCodecFactory factory = new CompressionCodecFactory(this.getFileSystem().getConf());
      CompressionCodec codec = factory.getCodec(p);
      return (codec == null) ? in : codec.createInputStream(in);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot open file " + path + " due to " + e.getMessage(), e);
    }
  }

  public Schema getAvroSchema(String file) throws FileBasedHelperException {
    DataFileReader<GenericRecord> dfr = null;
    try {
      if (this.getState().getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
          ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
        dfr =
            new DataFileReader<>(new ProxyFsInput(new Path(file), this.getFileSystem()),
                new GenericDatumReader<GenericRecord>());
      } else {
        dfr =
            new DataFileReader<>(new FsInput(new Path(file), this.getFileSystem().getConf()),
                new GenericDatumReader<GenericRecord>());
      }
      return dfr.getSchema();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    } finally {
      if (dfr != null) {
        try {
          dfr.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close avro file " + file, e);
        }
      }
    }
  }

  /**
   * Returns an {@link DataFileReader} to the specified avro file.
   * <p>
   * Note: It is the caller's responsibility to close the returned {@link DataFileReader}.
   * </p>
   * @param file The path to the avro file to open.
   * @return A {@link DataFileReader} for the specified avro file.
   * @throws FileBasedHelperException if there is a problem opening the {@link InputStream} for the specified file.
   */
  public DataFileReader<GenericRecord> getAvroFile(String file) throws FileBasedHelperException {
    try {
      if (!this.getFileSystem().exists(new Path(file))) {
        LOGGER.warn(file + " does not exist.");
        return null;
      }
      if (this.getState().getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
          ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
        return new DataFileReader<>(new ProxyFsInput(new Path(file), this.getFileSystem()),
            new GenericDatumReader<GenericRecord>());
      } else {
        return new DataFileReader<>(new FsInput(new Path(file), this.getFileSystem().getConf()),
            new GenericDatumReader<GenericRecord>());
      }
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      return this.getFileSystem().getFileStatus(new Path(filePath)).getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException(String.format("Failed to get size for file at path %s due to error %s",
          filePath, e.getMessage()), e);
    }
  }
}
