/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import gobblin.source.extractor.filebased.FileBasedHelper;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.util.HadoopUtils;

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


public class HadoopFsHelper implements FileBasedHelper {
  private static Logger log = LoggerFactory.getLogger(HadoopFsHelper.class);
  private State state;
  private final Configuration configuration;
  private FileSystem fs;

  public HadoopFsHelper(State state) {
    this(state, HadoopUtils.newConfiguration());
  }

  public HadoopFsHelper(State state, Configuration configuration) {
    this.state = state;
    this.configuration = configuration;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  @Override
  public void connect()
      throws FileBasedHelperException {
    URI uri = null;
    try {
      uri = new URI(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI));
      this.fs = FileSystem.get(uri, configuration);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot connect to given URI " + uri + " due to " + e.getMessage(), e);
    } catch (URISyntaxException e) {
      throw new FileBasedHelperException("Malformed uri " + uri + " due to " + e.getMessage(), e);
    }
  }

  @Override
  public void close()
      throws FileBasedHelperException {
        /*
         * TODO
         * Removing this for now, FileSystem.get() returns the same object each time it is called within a process
         * If this method gets called in parallel across tasks it is going to cause problems
         * Basically, close cannot be called multiple times within a process
         * http://stackoverflow.com/questions/17421218/multiples-hadoop-filesystem-instances
         */

//        try {
//            this.fs.close();
//        } catch (IOException e) {
//            throw new FileBasedHelperException("Cannot close Hadoop filesystem due to "  + e.getMessage(), e);
//        }
  }

  @Override
  public List<String> ls(String path)
      throws FileBasedHelperException {
    List<String> results = new ArrayList<String>();
    try {
      lsr(new Path(path), results);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot do ls on path " + path + " due to " + e.getMessage(), e);
    }
    return results;
  }

  public void lsr(Path p, List<String> results)
      throws IOException {
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
  public InputStream getFileStream(String path)
      throws FileBasedHelperException {
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

  public Schema getAvroSchema(String file)
      throws FileBasedHelperException {
    DataFileReader<GenericRecord> dfr = null;
    try {
      dfr = new DataFileReader<GenericRecord>(new FsInput(new Path(file), fs.getConf()),
          new GenericDatumReader<GenericRecord>());
      return dfr.getSchema();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    } finally {
      if (dfr != null) {
        try {
          dfr.close();
        } catch (IOException e) {
          log.error("Failed to close avro file " + file, e);
        }
      }
    }
  }

  public DataFileReader<GenericRecord> getAvroFile(String file)
      throws FileBasedHelperException {
    try {
      return new DataFileReader<GenericRecord>(new FsInput(new Path(file), fs.getConf()),
          new GenericDatumReader<GenericRecord>());
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open avro file " + file + " due to error " + e.getMessage(), e);
    }
  }
}
