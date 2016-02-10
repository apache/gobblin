/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.extractor;

import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;


/**
 * An implementation of {@link Extractor} that extracts {@link InputStream}s. This extractor is suitable for copy jobs
 * where files from any source to a sink. The extractor extracts a {@link FileAwareInputStream} which encompasses an
 * {@link InputStream} and a {@link CopyableFile} for every file that needs to be copied.
 *
 * <p>
 * In Gobblin {@link Extractor} terms, each {@link FileAwareInputStream} is a record. i.e one record per copyable file.
 * The extractor is capable of extracting multiple files
 * <p>
 */
public class FileAwareInputStreamExtractor implements Extractor<String, FileAwareInputStream> {

  private final FileSystem fs;
  private final CopyableFile file;
  /** True indicates the unique record has already been read. */
  private boolean recordRead;

  public FileAwareInputStreamExtractor(FileSystem fs, CopyableFile file) throws IOException {

    this.fs = fs;
    this.file = file;
    this.recordRead = false;
  }

  /**
   * @return Constant string schema.
   * @throws IOException
   */
  @Override
  public String getSchema() throws IOException {
    return FileAwareInputStream.class.getName();
  }

  @Override
  public FileAwareInputStream readRecord(@Deprecated FileAwareInputStream reuse) throws DataRecordException,
      IOException {

    if (!this.recordRead) {
      this.recordRead = true;
      return new FileAwareInputStream(this.file, fs.open(this.file.getFileStatus().getPath()));
    } else {
      return null;
    }

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
  public void close() throws IOException {
  }
}
