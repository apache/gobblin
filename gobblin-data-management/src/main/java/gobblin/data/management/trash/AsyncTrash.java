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

package gobblin.data.management.trash;

import gobblin.util.Decorator;
import gobblin.util.ExecutorsUtils;
import gobblin.util.executors.ScalingThreadPoolExecutor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;


/**
 * Implementation of {@link Trash} that deletes files asynchronously and in parallel.
 *
 * <p>
 *   This implementation is not built through {@link TrashFactory} because coder must be aware that the trash
 *   implementation is asynchronous. However, internally it uses {@link TrashFactory} to instantiate the trash
 *   implementation that will actually perform the deletes. This class acts as a {@link Decorator} of the
 *   inner trash.
 * </p>
 *
 * <p>
 *   Trash methods will always return true, regardless of success of the actual trash operation. However, additional
 *   methods are provided to get a future for the operation.
 * </p>
 */
public class AsyncTrash implements GobblinProxiedTrash, Closeable, Decorator {

  public static final String MAX_DELETING_THREADS_KEY = "gobblin.trash.async.max.deleting.threads";
  public static final int DEFAULT_MAX_DELETING_THREADS = 100;

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTrash.class);

  private final ProxiedTrash innerTrash;
  private final ListeningExecutorService executor;

  public AsyncTrash(FileSystem fs, Properties properties) throws IOException {
    this(fs, properties, UserGroupInformation.getCurrentUser().getShortUserName());
  }

  public AsyncTrash(FileSystem fs, Properties properties, String user) throws IOException {

    int maxDeletingThreads = DEFAULT_MAX_DELETING_THREADS;
    if(properties.containsKey(MAX_DELETING_THREADS_KEY)) {
      maxDeletingThreads = Integer.parseInt(properties.getProperty(MAX_DELETING_THREADS_KEY));
    }
    this.innerTrash = TrashFactory.createProxiedTrash(fs, properties, user);
    this.executor = MoreExecutors.listeningDecorator(MoreExecutors.getExitingExecutorService(ScalingThreadPoolExecutor
            .newScalingThreadPool(0, maxDeletingThreads, 100,
                ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("Async-trash-delete-pool-%d")))));

  }

  @Override public boolean moveToTrashAsUser(Path path, String user) throws IOException {
    moveToTrashAsUserFuture(path, user);
    return true;
  }

  /**
   * Schedules a {@link ProxiedTrash#moveToTrashAsUser} and returns a future for this operation.
   * @param path {@link Path} to delete.
   * @param user User to execute the operation as.
   * @return true if operation succeeded.
   */
  public ListenableFuture<Boolean> moveToTrashAsUserFuture(final Path path, final String user) {
    return this.executor.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return innerTrash.moveToTrashAsUser(path, user);
      }
    });
  }

  public boolean moveToTrashAsOwner(Path path) throws IOException {
    moveToTrashAsOwnerFuture(path);
    return true;
  }

  /**
   * Schedules a {@link ProxiedTrash#moveToTrashAsOwner} and returns a future for this operation.
   * @param path {@link Path} to delete.
   * @return true if operation succeeded.
   */
  public ListenableFuture<Boolean> moveToTrashAsOwnerFuture(final Path path) {
    return this.executor.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return innerTrash.moveToTrashAsOwner(path);
      }
    });
  }

  @Override public boolean moveToTrash(Path path) throws IOException {
    moveToTrashFuture(path);
    return true;
  }

  /**
   * Schedules a {@link ProxiedTrash#moveToTrash} and returns a future for this operation.
   * @param path {@link Path} to delete.
   * @return true if operation succeeded.
   */
  public ListenableFuture<Boolean> moveToTrashFuture(final Path path) {
    return this.executor.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return innerTrash.moveToTrash(path);
      }
    });
  }

  @Override public Object getDecoratedObject() {
    return this.innerTrash;
  }

  @Override public void close() throws IOException {
    try {
      this.executor.shutdown();
      this.executor.awaitTermination(5, TimeUnit.HOURS);
    } catch (InterruptedException ie) {
      this.executor.shutdownNow();
    }
  }
}
