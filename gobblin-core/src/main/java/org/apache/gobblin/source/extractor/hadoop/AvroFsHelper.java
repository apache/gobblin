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

package org.apache.gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.io.InputStream;

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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.SizeAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.utils.ProxyFsInput;
import org.apache.gobblin.util.HadoopUtils;


public class AvroFsHelper extends HadoopFsHelper implements SizeAwareFileBasedHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroFsHelper.class);

  public AvroFsHelper(State state) {
    this(state, HadoopUtils.newConfiguration());
  }

  public AvroFsHelper(State state, Configuration configuration) {
    super(state, configuration);
  }


  public Schema getAvroSchema(String file) throws FileBasedHelperException {
    DataFileReader<GenericRecord> dfr = null;
    try {
      if (this.getState().getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
          ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
        dfr = new DataFileReader<>(new ProxyFsInput(new Path(file), this.getFileSystem()),
            new GenericDatumReader<GenericRecord>());
      } else {
        dfr = new DataFileReader<>(new FsInput(new Path(file), this.getFileSystem().getConf()),
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
   *
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
      }
      return new DataFileReader<>(new FsInput(new Path(file), this.getFileSystem().getConf()),
          new GenericDatumReader<GenericRecord>());
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      return this.getFileSystem().getFileStatus(new Path(filePath)).getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException(
          String.format("Failed to get size for file at path %s due to error %s", filePath, e.getMessage()), e);
    }
  }
}
