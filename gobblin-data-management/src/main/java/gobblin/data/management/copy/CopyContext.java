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

package gobblin.data.management.copy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


/**
 * Context that can hold global objects required in a single copy job.
 */
public class CopyContext {

  /**
   * Cache for {@link FileStatus}es for various paths in {@link org.apache.hadoop.fs.FileSystem}s. Used to reduce
   * the number of calls to {@link org.apache.hadoop.fs.FileSystem#getFileStatus} when replicating attributes. Keys
   * should be fully qualified paths in case multiple {@link org.apache.hadoop.fs.FileSystem}s are in use.
   */
  private final Cache<Path, Optional<FileStatus>> fileStatusCache;

  public CopyContext() {
    this.fileStatusCache = CacheBuilder.newBuilder().maximumSize(10000).build();
  }

  /**
   * Get cached {@link FileStatus}.
   */
  public Optional<FileStatus> getFileStatus(final FileSystem fs, final Path path) throws IOException {
    try {
      return this.fileStatusCache.get(fs.makeQualified(path), new Callable<Optional<FileStatus>>() {
        @Override
        public Optional<FileStatus> call()
            throws Exception {
          try {
            return Optional.of(fs.getFileStatus(path));
          } catch (FileNotFoundException fnfe) {
            return Optional.absent();
          }
        }
      });
    } catch (ExecutionException ee) {
      throw new IOException(ee.getCause());
    }
  }

}
