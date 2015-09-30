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

package gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.RateBasedLimiter;
import gobblin.util.Decorator;


/**
 * Subclass of {@link org.apache.hadoop.fs.FileSystem} that wraps with a {@link gobblin.util.limiter.Limiter}
 * to control HDFS call rate.
 *
 *  <p>
 *  This classes uses Guava's {@link Cache} for storing {@link org.apache.hadoop.fs.FileSystem} URI to
 *  {@link gobblin.util.limiter.Limiter} mapping.
 *  </p>
 *
 *  <p>
 *  For methods that require HDFS calls, this class will first acquire a permit using {@link gobblin.util.limiter.Limiter},
 *  to make sure HDFS call rate is allowed by the uppper limit.
 *  </p>
 */
public class RateControlledFileSystem extends FileSystem implements Decorator {

  private static final int DEFAULT_MAX_CACHE_SIZE = 100;
  private static final Cache<String, Limiter> FS_URI_TO_RATE_LIMITER_CACHE = CacheBuilder.newBuilder()
      .maximumSize(DEFAULT_MAX_CACHE_SIZE).build();

  private final FileSystem fs;
  private final Callable<Limiter> callableLimiter;

  public RateControlledFileSystem(FileSystem fs, final long limitPerSecond) {
    this.fs = fs;
    this.callableLimiter = new Callable<Limiter>() {
      @Override
      public Limiter call() throws Exception {
        return new RateBasedLimiter(limitPerSecond);
      }
    };
  }

  @Override
  public boolean delete(Path path) throws IOException {
    return this.delete(path, true);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    this.acquirePermit();
    return this.fs.delete(path, recursive);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    this.acquirePermit();
    return this.fs.exists(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    this.acquirePermit();
    return this.fs.getFileStatus(path);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    this.acquirePermit();
    return this.fs.globStatus(pathPattern);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    this.acquirePermit();
    return this.fs.listStatus(path);
  }

  @Override
  public FileStatus[] listStatus(Path path, PathFilter filter) throws IOException {
    this.acquirePermit();
    return this.fs.listStatus(path, filter);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    this.acquirePermit();
    return this.fs.mkdirs(path, permission);
  }

  @Override
  public boolean rename(Path path0, Path path1) throws IOException {
    this.acquirePermit();
    return this.fs.rename(path0, path1);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
    this.acquirePermit();
    return this.fs.append(path, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    this.acquirePermit();
    return this.fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public URI getUri() {
    return this.fs.getUri();
  }

  @Override
  public Path getWorkingDirectory() {
    return this.fs.getWorkingDirectory();
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    this.acquirePermit();
    return this.fs.open(path, bufferSize);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    this.fs.setWorkingDirectory(path);
  }

  @Override
  public Configuration getConf() {
    return this.fs.getConf();
  }

  public void startRateControl() throws ExecutionException {
    getRateLimiter().start();
  }

  private void acquirePermit() throws IOException {
    try {
      getRateLimiter().acquirePermits(1);
    } catch (InterruptedException e) {
      throw new IOException("Failed to acquire rate limit.", e);
    } catch (ExecutionException e) {
      throw new IOException("Failed to acquire rate limit.", e);
    }
  }

  private Limiter getRateLimiter() throws ExecutionException {
    return FS_URI_TO_RATE_LIMITER_CACHE.get(this.fs.getUri().toString(), this.callableLimiter);
  }

  @Override
  public Object getDecoratedObject() {
    return this.fs;
  }

  @Override
  public void close() throws IOException {
    try {
      getRateLimiter().stop();
    } catch (ExecutionException e) {
      throw new IOException("Failed to stop rate limiter", e);
    }
  }
}
