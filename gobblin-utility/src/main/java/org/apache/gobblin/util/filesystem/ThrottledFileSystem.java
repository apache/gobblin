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

package org.apache.gobblin.util.filesystem;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.NotEnoughPermitsException;
import org.apache.gobblin.util.limiter.broker.SharedLimiterFactory;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A {@link FileSystemInstrumentation} for throttling calls to the underlying {@link FileSystem} using the input
 * {@link Limiter}.
 */
public class ThrottledFileSystem extends FileSystemInstrumentation {

  /**
   * Factory for {@link ThrottledFileSystem}.
   */
  public static class Factory<S extends ScopeType<S>> extends FileSystemInstrumentationFactory<S> {
    private static final String SERVICE_NAME_CONF_KEY = "gobblin.broker.filesystem.limiterServiceName";
    @Override
    public FileSystem instrumentFileSystem(FileSystem fs, SharedResourcesBroker<S> broker,
        ConfigView<S, FileSystemKey> config) {
      try {
        String serviceName = ConfigUtils.getString(config.getConfig(), SERVICE_NAME_CONF_KEY, "");
        Limiter limiter = broker.getSharedResource(new SharedLimiterFactory<S>(), new FileSystemLimiterKey(config.getKey().getUri()));
        return new ThrottledFileSystem(fs, limiter, serviceName);
      } catch (NotConfiguredException nce) {
        throw new RuntimeException(nce);
      }
    }
  }

  /**
   * Listing operations will use 1 permit per this many listed elements.
   */
  public static final int LISTING_FILES_PER_PERMIT = 100;

  private final Limiter limiter;
  private final String serviceName;

  public ThrottledFileSystem(FileSystem fs, Limiter limiter, String serviceName) {
    super(fs);
    this.limiter = limiter;
    this.serviceName = serviceName;
  }

  @Override
  public boolean delete(Path path) throws IOException {
    return this.delete(path, true);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    this.acquirePermit("delete " + path);
    return super.delete(path, recursive);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    this.acquirePermit("exists " + path);
    return super.exists(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    this.acquirePermit("getFileStatus " + path);
    return super.getFileStatus(path);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    FileStatus[] statuses = super.globStatus(pathPattern);
    if (statuses == null) {
      acquirePermit("globStatus " + pathPattern);
    } else {
      acquirePermits(statuses.length / LISTING_FILES_PER_PERMIT + 1, "globStatus " + pathPattern);
    }
    return statuses;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FileStatus[] statuses = super.listStatus(path);
    if (statuses == null) {
      acquirePermit("listStatus " + path);
    } else {
      acquirePermits(statuses.length / LISTING_FILES_PER_PERMIT + 1, "listStatus " + path);
    }
    return statuses;
  }

  @Override
  public FileStatus[] listStatus(Path path, PathFilter filter) throws IOException {
    CountingPathFilterDecorator decoratedFilter = new CountingPathFilterDecorator(filter);
    FileStatus[] statuses = super.listStatus(path, decoratedFilter);
    if (statuses == null) {
      acquirePermit("listStatus " + path);
    } else {
      acquirePermits(decoratedFilter.getPathsProcessed().get() / LISTING_FILES_PER_PERMIT + 1, "listStatus " + path);
    }
    return statuses;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    this.acquirePermit("mkdirs " + path);
    return super.mkdirs(path, permission);
  }

  @Override
  public boolean rename(Path path0, Path path1) throws IOException {
    this.acquirePermit("rename " + path0);
    return HadoopUtils.renamePathHandleLocalFSRace(this.underlyingFs, path0, path1);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
    this.acquirePermit("append " + path);
    return super.append(path, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    this.acquirePermit("create " + path);
    return super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    this.acquirePermit("open " + path);
    return super.open(path, bufferSize);
  }

  private void acquirePermit(String op) throws IOException {
    acquirePermits(1, op);
  }

  private void acquirePermits(int permits, String op) throws IOException {
    try {
      Closeable closeable = getRateLimiter().acquirePermits(permits);
      if (closeable == null) {
        throw new NotEnoughPermitsException(op);
      }
    } catch (InterruptedException e) {
      throw new NotEnoughPermitsException(op, e);
    }
  }

  protected Limiter getRateLimiter() {
    return this.limiter;
  }

  public String getServiceName() {
    return this.serviceName;
  }

  @Override
  public void close() throws IOException {
    getRateLimiter().stop();
    super.close();
  }

  @RequiredArgsConstructor
  private static class CountingPathFilterDecorator implements PathFilter {
    private final PathFilter underlying;
    @Getter
    private final AtomicInteger pathsProcessed = new AtomicInteger();

    @Override
    public boolean accept(Path path) {
      this.pathsProcessed.incrementAndGet();
      return this.underlying.accept(path);
    }
  }
}
