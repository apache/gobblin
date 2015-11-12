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
package gobblin.data.management.copy.extractor;

import gobblin.data.management.copy.CopyableFile;
import gobblin.source.extractor.extract.sftp.SftpLightWeightFileSystem;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.io.Closer;


/**
 * Used instead of {@link FileAwareInputStreamExtractor} for {@link FileSystem}s that need be closed after use E.g
 * {@link SftpLightWeightFileSystem}.
 */
public class CloseableFsFileAwareInputStreamExtractor extends FileAwareInputStreamExtractor {

  private final Closer closer = Closer.create();

  public CloseableFsFileAwareInputStreamExtractor(FileSystem fs, Iterator<CopyableFile> filesIterator)
      throws IOException {

    super(fs, filesIterator);
    closer.register(fs);
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }
}
