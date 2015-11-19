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

package gobblin.yarn;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.io.CharStreams;


/**
 * An implementation of {@link ApplicationIdStore} that stores the single
 * {@link org.apache.hadoop.yarn.api.records.ApplicationId} in its string
 * form in a file on a {@link org.apache.hadoop.fs.FileSystem}.
 *
 * @author ynli
 */
class FsApplicationIdStore implements ApplicationIdStore {

  private final FileSystem fs;
  private final Path applicationIdFilePath;

  public FsApplicationIdStore(FileSystem fs, Path applicationIdFilePath) {
    this.fs = fs;
    this.applicationIdFilePath = applicationIdFilePath;
  }

  @Override
  public void put(String applicationId) throws IOException {
    delete();

    try (OutputStreamWriter osWriter = new OutputStreamWriter(this.fs.create(this.applicationIdFilePath))) {
      if (!this.fs.exists(this.applicationIdFilePath.getParent())) {
        this.fs.mkdirs(this.applicationIdFilePath.getParent());
      }

      CharStreams.asWriter(osWriter).write(applicationId);
    }
  }

  @Override
  public Optional<String> get() throws IOException {
    if (!this.fs.exists(this.applicationIdFilePath)) {
      return Optional.absent();
    }

    try (InputStreamReader isReader = new InputStreamReader(this.fs.open(this.applicationIdFilePath))) {
      return Optional.of(CharStreams.toString(isReader));
    }
  }

  @Override
  public void delete() throws IOException {
    if (this.fs.exists(this.applicationIdFilePath)) {
      this.fs.delete(this.applicationIdFilePath, false);
    }
  }

  @Override
  public void close() throws IOException {
    delete();
  }
}
