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

package gobblin.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link org.apache.hadoop.fs.FileSystem} that extends HDFS and allows instrumentation of certain calls (for example,
 * counting the number of calls to a certain method or measuring latency). For now it is just a skeleton.
 *
 * Using the scheme "instrumented-hdfs" will automatically use this {@link org.apache.hadoop.fs.FileSystem} and work
 * transparently as any other HDFS file system.
 *
 * When modifying this class, tests must be run manually (see InstrumentedHDFSFileSystemTest).
 */
@Slf4j
public class InstrumentedHDFSFileSystem extends DistributedFileSystem {

  public static final String INSTRUMENTED_HDFS_SCHEME = "instrumented-hdfs";

  private static final String HDFS_SCHEME = "hdfs";

  @Override
  public String getScheme() {
    return INSTRUMENTED_HDFS_SCHEME;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(replaceScheme(uri, INSTRUMENTED_HDFS_SCHEME, HDFS_SCHEME), conf);
  }

  @Override
  protected URI getCanonicalUri() {
    return replaceScheme(super.getCanonicalUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public URI getUri() {
    return replaceScheme(super.getUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return replaceScheme(super.canonicalizeUri(uri), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    // Should print out statistics here
  }

  private URI replaceScheme(URI uri, String replace, String replacement) {
    try {
      if (replace != null && replace.equals(uri.getScheme())) {
        return new URI(replacement, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
      } else {
        return uri;
      }
    } catch (URISyntaxException use) {
      throw new RuntimeException("Failed to replace scheme.");
    }
  }
}
