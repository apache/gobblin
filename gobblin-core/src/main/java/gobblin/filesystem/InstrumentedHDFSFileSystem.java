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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import com.google.common.io.Closer;
import gobblin.metrics.MetricContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
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
  public static final String HDFS_METRIC_CONTEXT_NAME = "hdfsMetricContext";
  private static final String HDFS_SCHEME = "hdfs";
  private MetricContext metricContext;

  protected final Closer closer;

  // Below are HDFS metrics
  @VisibleForTesting
  protected final Timer listStatusPathTimer;
  @VisibleForTesting
  protected final Timer listStatusPathsTimer;
  @VisibleForTesting
  protected final Timer listStatusPathWithFilterTimer;
  @VisibleForTesting
  protected final Timer listStatusPathsWithFilterTimer;
  @VisibleForTesting
  protected final Timer listFilesTimer;
  @VisibleForTesting
  protected final Timer globStatusTimer;
  @VisibleForTesting
  protected final Timer mkdirTimer;
  @VisibleForTesting
  protected final Timer deleteTimer;
  @VisibleForTesting
  protected final Timer renameTimer;


  public InstrumentedHDFSFileSystem() {
    this.closer = Closer.create();
    this.metricContext = new MetricContext.Builder(HDFS_METRIC_CONTEXT_NAME).build();
    this.metricContext = this.closer.register(metricContext);

    this.listStatusPathTimer = this.metricContext.timer("listStatusPath");
    this.listStatusPathsTimer = this.metricContext.timer("listStatusPaths");
    this.listStatusPathWithFilterTimer = this.metricContext.timer("listStatusPathWithFilter");
    this.listStatusPathsWithFilterTimer = this.metricContext.timer("listStatusPathsWithFilter");
    this.listFilesTimer = this.metricContext.timer("listFilesTimer");
    this.globStatusTimer = this.metricContext.timer("globStatusTimer");
    this.mkdirTimer = this.metricContext.timer("mkdirs");
    this.renameTimer = this.metricContext.timer("rename");
    this.deleteTimer = this.metricContext.timer("deleteTimer");
  }

  @Override
  public String getScheme() {
    return INSTRUMENTED_HDFS_SCHEME;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(InstrumentedFileSystemUtils.replaceScheme(uri, INSTRUMENTED_HDFS_SCHEME, HDFS_SCHEME), conf);
  }

  @Override
  protected URI getCanonicalUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getCanonicalUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public URI getUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return InstrumentedFileSystemUtils.replaceScheme(super.canonicalizeUri(uri), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    // Should print out statistics here
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#mkdir(Path, FsPermission)}
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    try (Closeable context = mkdirTimer.time()) {
      boolean status = super.mkdir (f, permission);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Indirect call to {@link InstrumentedHDFSFileSystem#mkdirs(Path, FsPermission)}
   */
  public boolean mkdirs(Path f) throws IOException {
    return super.mkdirs(f);
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#mkdirs(Path, FsPermission)}
   */
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (Closeable context = mkdirTimer.time()) {
      boolean status = super.mkdirs (f, permission);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#rename(Path, Path)}
   */
  public boolean rename (Path src, Path dst) throws IOException {
    try (Closeable context =  renameTimer.time()) {
      boolean status = super.rename(src, dst);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#delete(Path, boolean)}
   */
  public boolean delete (Path f, boolean recursive) throws  IOException {
    try (Closeable context = deleteTimer.time()) {
      boolean status = super.delete (f, recursive);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#listStatus(Path)}
   */
  public FileStatus[] listStatus(Path path) throws IOException {
    try (Closeable context = listStatusPathTimer.time()) {
      FileStatus[] status = super.listStatus(path);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#listStatus(Path[])}
   */
  public FileStatus[] listStatus(Path[] paths) throws IOException {
    try (Closeable context = listStatusPathsTimer.time()) {
      FileStatus[] status = super.listStatus(paths);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#listStatus(Path, PathFilter)}
   */
  public FileStatus[] listStatus(Path path, PathFilter filter) throws IOException {
    try (Closeable context = listStatusPathWithFilterTimer.time()) {
      FileStatus[] status = super.listStatus(path, filter);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#listStatus(Path[], PathFilter)}
   */
  public FileStatus[] listStatus(Path[] paths, PathFilter filter) throws IOException {
    try (Closeable context = listStatusPathsWithFilterTimer.time()) {
      FileStatus[] status = super.listStatus(paths, filter);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#globStatus(Path)}
   */
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    try (Closeable context = globStatusTimer.time()) {
      FileStatus[] status = super.globStatus(pathPattern);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#globStatus(Path, PathFilter)}
   */
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    try (Closeable context = globStatusTimer.time()) {
      FileStatus[] status = super.globStatus(pathPattern, filter);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link FileSystem#listFiles(Path, boolean)}
   */
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException{
    try (Closeable context = this.listFilesTimer.time()) {
      RemoteIterator<LocatedFileStatus>  status = super.listFiles(f, recursive);
      return status;
    } catch (IOException e) {
      throw e;
    }
  }
}
