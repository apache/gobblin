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

package gobblin.filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link org.apache.hadoop.fs.FileSystem} that extends webHDFS and allows instrumentation of certain calls (for example,
 * counting the number of calls to a certain method or measuring latency). For now it is just a skeleton.
 *
 * Using the scheme "instrumented-webhdfs" will automatically use this {@link org.apache.hadoop.fs.FileSystem} and work
 * transparently as any other HDFS file system.
 *
 * When modifying this class, tests must be run manually (see InstrumentedHDFSFileSystemTest).
 */
@Slf4j
public class InstrumentedWebHdfsFileSystem extends WebHdfsFileSystem {

  public static final String INSTRUMENTED_WEB_HDFS_SCHEME = "instrumented-webhdfs";

  @Override
  public String getScheme() {
    return INSTRUMENTED_WEB_HDFS_SCHEME;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(InstrumentedFileSystemUtils.replaceScheme(uri, INSTRUMENTED_WEB_HDFS_SCHEME, WebHdfsFileSystem.SCHEME), conf);
  }

  @Override
  public URI getCanonicalUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getCanonicalUri(), WebHdfsFileSystem.SCHEME, INSTRUMENTED_WEB_HDFS_SCHEME);
  }

  @Override
  public URI getUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getUri(), WebHdfsFileSystem.SCHEME, INSTRUMENTED_WEB_HDFS_SCHEME);
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return InstrumentedFileSystemUtils.replaceScheme(super.canonicalizeUri(uri), WebHdfsFileSystem.SCHEME,
        INSTRUMENTED_WEB_HDFS_SCHEME);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    // Should print out statistics here
  }
}
