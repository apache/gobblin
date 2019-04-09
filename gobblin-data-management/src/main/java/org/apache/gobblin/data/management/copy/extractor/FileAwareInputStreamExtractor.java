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

package org.apache.gobblin.data.management.copy.extractor;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.io.EmptyInputStream;
import org.apache.gobblin.util.io.MeteredInputStream;


/**
 * An implementation of {@link Extractor} that extracts {@link InputStream}s. This extractor is suitable for copy jobs
 * where files from any source to a sink. The extractor extracts a {@link FileAwareInputStream} which encompasses an
 * {@link InputStream} and a {@link org.apache.gobblin.data.management.copy.CopyEntity} for every file that needs to be copied.
 *
 * <p>
 * In Gobblin {@link Extractor} terms, each {@link FileAwareInputStream} is a record. i.e one record per copyable file.
 * The extractor is capable of extracting multiple files
 * <p>
 */
public class FileAwareInputStreamExtractor implements Extractor<String, FileAwareInputStream> {

  protected final FileSystem fs;
  protected final CopyableFile file;
  protected final WorkUnitState state;
  /** True indicates the unique record has already been read. */
  protected boolean recordRead;

  public FileAwareInputStreamExtractor(FileSystem fs, CopyableFile file, WorkUnitState state) {
    this.fs = fs;
    this.file = file;
    this.state = state;
    this.recordRead = false;
  }

  public FileAwareInputStreamExtractor(FileSystem fs, CopyableFile file) {
    this(fs, file, null);
  }

  /**
   * @return Constant string schema.
   * @throws IOException
   */
  @Override
  public String getSchema()
      throws IOException {
    return FileAwareInputStream.class.getName();
  }

  @Override
  public FileAwareInputStream readRecord(@Deprecated FileAwareInputStream reuse)
      throws DataRecordException, IOException {

    if (!this.recordRead) {
      Configuration conf =
          this.state == null ? HadoopUtils.newConfiguration() : HadoopUtils.getConfFromState(this.state);
      FileSystem fsFromFile = this.file.getOrigin().getPath().getFileSystem(conf);
      return buildStream(fsFromFile);
    }
    return null;
  }

  protected FileAwareInputStream buildStream(FileSystem fsFromFile)
      throws DataRecordException, IOException{
    this.recordRead = true;
    FileAwareInputStream.FileAwareInputStreamBuilder builder = FileAwareInputStream.builder().file(this.file);
    if (this.file.getFileStatus().isDirectory()) {
      return builder.inputStream(EmptyInputStream.instance).build();
    }

    FSDataInputStream dataInputStream = fsFromFile.open(this.file.getFileStatus().getPath());
    if (this.state != null && DistcpFileSplitter.isSplitWorkUnit(this.state)) {
      Optional<DistcpFileSplitter.Split> split = DistcpFileSplitter.getSplit(this.state);
      builder.split(split);
      if (split.isPresent()) {
        dataInputStream.seek(split.get().getLowPosition());
      }
    }
    builder.inputStream(MeteredInputStream.builder().in(dataInputStream).build());
    return builder.build();
  }

  /**
   * Each {@link FileAwareInputStreamExtractor} processes exactly one record.
   */
  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
  }
}
