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

package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.common.base.Strings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelper;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import gobblin.util.HadoopUtils;
import gobblin.util.ProxiedFileSystemWrapper;


/**
 * A common helper that extends {@link FileBasedHelper} and provides access to a files via a {@link FileSystem}.
 */
public class HadoopFsHelper implements TimestampAwareFileBasedHelper {
  private final State state;
  private final Configuration configuration;
  private FileSystem fs;

  public HadoopFsHelper(State state) {
    this(state, HadoopUtils.getConfFromState(state));
  }

  public HadoopFsHelper(State state, Configuration configuration) {
    this.state = state;
    this.configuration = configuration;
  }

  protected State getState() {
    return this.state;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  @Override
  public void connect() throws FileBasedHelperException {
    String uri = this.state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI);
    try {
      if (Strings.isNullOrEmpty(uri)) {
        throw new FileBasedHelperException(ConfigurationKeys.SOURCE_FILEBASED_FS_URI + " has not been configured");
      }
      this.createFileSystem(uri);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot connect to given URI " + uri + " due to " + e.getMessage(), e);
    } catch (URISyntaxException e) {
      throw new FileBasedHelperException("Malformed uri " + uri + " due to " + e.getMessage(), e);
    } catch (InterruptedException e) {
      throw new FileBasedHelperException("Interrupted exception is caught when getting the proxy file system", e);
    }
  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
    List<String> results = new ArrayList<>();
    try {
      lsr(new Path(path), results);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot do ls on path " + path + " due to " + e.getMessage(), e);
    }
    return results;
  }

  public void lsr(Path p, List<String> results) throws IOException {
    if (!this.fs.getFileStatus(p).isDirectory()) {
      results.add(p.toString());
    }
    Path qualifiedPath = this.fs.makeQualified(p);
    for (FileStatus status : this.fs.listStatus(p)) {
      if (status.isDirectory()) {
        // Fix for hadoop issue: https://issues.apache.org/jira/browse/HADOOP-12169
        if (!qualifiedPath.equals(status.getPath())) {
          lsr(status.getPath(), results);
        }
      } else {
        results.add(status.getPath().toString());
      }
    }
  }

  private void createFileSystem(String uri) throws IOException, InterruptedException, URISyntaxException {
    if (this.state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system as a proxy user.
      this.fs = new ProxiedFileSystemWrapper().getProxiedFileSystem(this.state, ProxiedFileSystemWrapper.AuthType.TOKEN,
          this.state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE), uri, configuration);

    } else {
      // Initialize file system as the current user.
      this.fs = FileSystem.newInstance(URI.create(uri), this.configuration);
    }
  }

  @Override
  public long getFileMTime(String filePath) throws FileBasedHelperException {
    try {
      return this.getFileSystem().getFileStatus(new Path(filePath)).getModificationTime();
    } catch (IOException e) {
      throw new FileBasedHelperException(String
          .format("Failed to get last modified time for file at path %s due to error %s", filePath, e.getMessage()), e);
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      return this.getFileSystem().getFileStatus(new Path(filePath)).getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException(
          String.format("Failed to get size for file at path %s due to error %s", filePath, e.getMessage()), e);
    }
  }

  /**
   * Returns an {@link InputStream} to the specified file.
   * <p>
   * Note: It is the caller's responsibility to close the returned {@link InputStream}.
   * </p>
   *
   * @param path The path to the file to open.
   * @return An {@link InputStream} for the specified file.
   * @throws FileBasedHelperException if there is a problem opening the {@link InputStream} for the specified file.
   */
  @Override
  public InputStream getFileStream(String path) throws FileBasedHelperException {
    try {
      Path p = new Path(path);
      InputStream in = this.getFileSystem().open(p);
      // Account for compressed files (e.g. gzip).
      // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/input/WholeTextFileRecordReader.scala
      CompressionCodecFactory factory = new CompressionCodecFactory(this.getFileSystem().getConf());
      CompressionCodec codec = factory.getCodec(p);
      return (codec == null) ? in : codec.createInputStream(in);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot open file " + path + " due to " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws IOException {
    this.getFileSystem().close();
  }
}
