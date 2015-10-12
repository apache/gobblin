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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import gobblin.util.ProxiedFileSystemWrapper;


public class AvroFsHelper implements SizeAwareFileBasedHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroFsHelper.class);

  private State state;
  private final Configuration configuration;
  private FileSystem fs;

  public AvroFsHelper(State state) {
    this(state, HadoopUtils.newConfiguration());
  }

  public AvroFsHelper(State state, Configuration configuration) {
    this.state = state;
    this.configuration = configuration;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  private void createFileSystem(String uri) throws IOException, InterruptedException, URISyntaxException {

    if (state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system as a proxy user.
      this.fs =
          new ProxiedFileSystemWrapper().getProxiedFileSystem(state, ProxiedFileSystemWrapper.AuthType.TOKEN,
              state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE), uri);

    } else {
      // Initialize file system as the current user.
      this.fs = FileSystem.get(URI.create(uri), this.configuration);
    }
  }

  @Override
  public void connect() throws FileBasedHelperException {
    URI uri = null;
    try {
      this.createFileSystem(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI));
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot connect to given URI " + uri + " due to " + e.getMessage(), e);
    } catch (URISyntaxException e) {
      throw new FileBasedHelperException("Malformed uri " + uri + " due to " + e.getMessage(), e);
    } catch (InterruptedException e) {
      throw new FileBasedHelperException("Interruptted exception is caught when getting the proxy file system", e);
    }
  }

  @Override
  public void close() throws FileBasedHelperException {

  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
    List<String> results = new ArrayList<String>();
    try {
      lsr(new Path(path), results);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot do ls on path " + path + " due to " + e.getMessage(), e);
    }
    return results;
  }

  public void lsr(Path p, List<String> results) throws IOException {
    if (!this.fs.getFileStatus(p).isDir()) {
      results.add(p.toString());
    }
    for (FileStatus status : this.fs.listStatus(p)) {
      if (status.isDir()) {
        lsr(status.getPath(), results);
      } else {
        results.add(status.getPath().toString());
      }
    }
  }

  @Override
  public InputStream getFileStream(String path) throws FileBasedHelperException {
    try {
      Path p = new Path(path);
      InputStream in = this.fs.open(p);
      // Account for compressed files (e.g. gzip).
      // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/input/WholeTextFileRecordReader.scala
      CompressionCodecFactory factory = new CompressionCodecFactory(this.fs.getConf());
      CompressionCodec codec = factory.getCodec(p);
      return (codec == null) ? in : codec.createInputStream(in);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot do open file " + path + " due to " + e.getMessage(), e);
    }
  }

  public Schema getAvroSchema(String file) throws FileBasedHelperException {
    DataFileReader<GenericRecord> dfr = null;
    try {
      if (state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
          ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
        dfr =
            new DataFileReader<GenericRecord>(new ProxyFsInput(new Path(file), this.fs),
                new GenericDatumReader<GenericRecord>());
      } else {
        dfr =
            new DataFileReader<GenericRecord>(new FsInput(new Path(file), fs.getConf()),
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

  public DataFileReader<GenericRecord> getAvroFile(String file) throws FileBasedHelperException {
    try {
      if (!fs.exists(new Path(file))) {
        LOGGER.warn(file + " does not exist.");
        return null;
      }
      if (state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
          ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
        return new DataFileReader<GenericRecord>(new ProxyFsInput(new Path(file), this.fs),
            new GenericDatumReader<GenericRecord>());
      } else {
        return new DataFileReader<GenericRecord>(new FsInput(new Path(file), fs.getConf()),
            new GenericDatumReader<GenericRecord>());
      }
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      return fs.getFileStatus(new Path(filePath)).getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException(String.format("Failed to get size for file at path %s due to error %s",
          filePath, e.getMessage()), e);
    }
  }
}
