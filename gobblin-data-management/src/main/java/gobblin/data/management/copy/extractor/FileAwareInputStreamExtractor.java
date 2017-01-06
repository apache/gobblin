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

package gobblin.data.management.copy.extractor;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


/**
 * An implementation of {@link Extractor} that extracts {@link InputStream}s. This extractor is suitable for copy jobs
 * where files from any source to a sink. The extractor extracts a {@link FileAwareInputStream} which encompasses an
 * {@link InputStream} and a {@link gobblin.data.management.copy.CopyEntity} for every file that needs to be copied.
 *
 * <p>
 * In Gobblin {@link Extractor} terms, each {@link FileAwareInputStream} is a record. i.e one record per copyable file.
 * The extractor is capable of extracting multiple files
 * <p>
 */
public class FileAwareInputStreamExtractor implements Extractor<String, FileAwareInputStream> {

  private final FileSystem fs;
  private final CopyableFile file;
  private final WorkUnitState state;
  /** True indicates the unique record has already been read. */
  private boolean recordRead;

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
      this.recordRead = true;
      return new FileAwareInputStream(this.file, fsFromFile.open(this.file.getFileStatus().getPath()));
    }
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
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
