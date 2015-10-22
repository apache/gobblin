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

import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.io.Closer;


public class InputStreamExtractor implements Extractor<String, FileAwareInputStream> {

  private final FileSystem fs;
  private Iterator<CopyableFile> fileIterator;
  private Closer closer;

  public InputStreamExtractor(FileSystem fs, Iterator<CopyableFile> fileIterator) throws IOException {
    this.closer = Closer.create();
    this.fs = closer.register(fs);
    this.fileIterator = fileIterator;
  }

  /**
   * @return Constant string schema.
   * @throws IOException
   */
  @Override
  public String getSchema() throws IOException {
    return "InputStream";
  }

  @Override
  public FileAwareInputStream readRecord(@Deprecated FileAwareInputStream reuse) throws DataRecordException,
      IOException {

    while (fileIterator.hasNext()) {
      CopyableFile file = fileIterator.next();
      return new FileAwareInputStream(file, fs.open(file.getFileStatus().getPath()));
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
  public void close() throws IOException {
    //closer.close();
  }
}
