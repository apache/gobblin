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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Striped;

import gobblin.configuration.State;


/**
 * A class that is responsible for running certain methods in parallel. Methods in this class returns immediately and
 * are run in a fixed-size thread pool.
 *
 * <p>
 *   This class is intended to be used in the following pattern. This example uses the serialize() method.
 *
 *   <pre> {@code
 *     Closer closer = Closer.create();
 *     try {
 *       // Do stuff
 *       ParallelRunner runner = closer.register(new ParallelRunner(threads, fs));
 *       runner.serialize(state1, outputFilePath1);
 *       // Submit more serialization tasks
 *       runner.serialize(stateN, outputFilePathN);
 *       // Do stuff
 *     } catch (Throwable e) {
 *       throw closer.rethrow(e);
 *     } finally {
 *       closer.close();
 *     }}
 *   </pre>
 *
 *   Note that calling {@link #close()} will wait for all submitted tasks to complete and then stop the
 *   {@link ParallelRunner} by shutting down the {@link ExecutorService}.
 * </p>
 *
 * @author ynli
 */
@Slf4j
public class ParallelRunner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelRunner.class);

  public static final String PARALLEL_RUNNER_THREADS_KEY = "parallel.runner.threads";
  public static final int DEFAULT_PARALLEL_RUNNER_THREADS = 10;

  private final ExecutorService executor;

  private final List<Future<?>> futures = Lists.newArrayList();

  private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

  public ParallelRunner(int threads) {
    this.executor = Executors.newFixedThreadPool(threads,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ParallelRunner")));
  }

  /**
   * Serialize a {@link State} object into a file.
   *
   * <p>
   *   This method submits a task to serialize the {@link State} object and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param state the {@link State} object to be serialized
   * @param fileSystem the {@link FileSystem} to write the file to
   * @param outputFilePath the file to write the serialized {@link State} object to
   * @param <T> the {@link State} object type
   */
  public <T extends State> void serializeToFile(final T state, final FileSystem fileSystem, final Path outputFilePath) {
    // Use a Callable with a Void return type to allow exceptions to be thrown
    this.futures.add(this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        SerializationUtils.serializeState(fileSystem, outputFilePath, state);
        return null;
      }
    }));
  }

  /**
   * Deserialize a {@link State} object from a file.
   *
   * <p>
   *   This method submits a task to deserialize the {@link State} object and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param state an empty {@link State} object to which the deserialized content will be populated
   * @param fileSystem the {@link FileSystem} to read the file from
   * @param inputFilePath the input file to read from
   * @param <T> the {@link State} object type
   */
  public <T extends State> void deserializeFromFile(final T state, final FileSystem fileSystem, final Path inputFilePath) {
    this.futures.add(this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        SerializationUtils.deserializeState(fileSystem, inputFilePath, state);
        return null;
      }
    }));
  }

  /**
   * Deserialize a list of {@link State} objects from a Hadoop {@link SequenceFile}.
   *
   * <p>
   *   This method submits a task to deserialize the {@link State} objects and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param stateClass the {@link Class} object of the {@link State} class
   * @param fileSystem the {@link FileSystem} to read the file from
   * @param inputFilePath the input {@link SequenceFile} to read from
   * @param states a {@link Collection} object to store the deserialized {@link State} objects
   * @param deleteAfter a flag telling whether to delete the {@link SequenceFile} afterwards
   * @param <T> the {@link State} object type
   */
  public <T extends State> void deserializeFromSequenceFile(final Class<? extends Writable> keyClass,
      final Class<T> stateClass, final FileSystem fileSystem, final Path inputFilePath, final Collection<T> states,
      final boolean deleteAfter) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Closer closer = Closer.create();
        try {
          @SuppressWarnings("deprecation")
          SequenceFile.Reader reader = closer.register(
                  new SequenceFile.Reader(fileSystem, inputFilePath, fileSystem.getConf()));
          Writable key = keyClass.newInstance();
          T state = stateClass.newInstance();
          while (reader.next(key, state)) {
            states.add(state);
            state = stateClass.newInstance();
          }

          if (deleteAfter) {
            HadoopUtils.deletePath(fileSystem, inputFilePath, false);
          }
        } catch (Throwable t) {
          throw closer.rethrow(t);
        } finally {
          closer.close();
        }

        return null;
      }
    }));
  }

  /**
   * Delete a {@link Path}.
   *
   * <p>
   *   This method submits a task to delete a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   * @param fileSystem the {@link FileSystem} to delete the path from
   * @param path path to be deleted.
   * @param recursive true if the delete is recursive; otherwise, false
   */
  public void deletePath(final FileSystem fileSystem, final Path path, final boolean recursive) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Lock lock = locks.get(path.toString());
        lock.lock();
        try {
          HadoopUtils.deletePath(fileSystem, path, recursive);
          return null;
        } finally {
          lock.unlock();
        }
      }
    }));
  }

  /**
   * Rename a {@link Path}.
   *
   * <p>
   *   This method submits a task to rename a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param fileSystem the {@link FileSystem} where the rename will be done
   * @param src path to be renamed
   * @param dst new path after rename
   * @param group an optional group name for the destination path
   *
   * @Deprecated Use {@link gobblin.util.ParallelRunner#movePath(FileSystem, Path, FileSystem, Path, Optional, Action)}
   */
  @Deprecated
  public void renamePath(final FileSystem fileSystem, final Path src, final Path dst, final Optional<String> group) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Lock lock = locks.get(src.toString());
        lock.lock();
        try {
          if (fileSystem.exists(src)) {
            HadoopUtils.renamePath(fileSystem, src, dst);
            if (group.isPresent()) {
              HadoopUtils.setGroup(fileSystem, dst, group.get());
            }
          }
          return null;
        } catch (FileAlreadyExistsException e) {
          LOGGER.warn(String.format("Failed to rename %s to %s: dst already exists", src, dst), e);
          return null;
        } finally {
          lock.unlock();
        }
      }
    }));
  }

  /**
   * Move a {@link Path}.
   *
   * <p>
   *   This method submits a task to move a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param srcFs the source {@link FileSystem}
   * @param src path to be moved
   * @param dstFs the destination {@link FileSystem}
   * @param dst the destination path
   * @param group an optional group name for the destination path
   * @param commitAction an action to perform when the move completes successfully
   */
  public void movePath(final FileSystem srcFs, final Path src, final FileSystem dstFs, final Path dst, final Optional<String> group,
                       final Action commitAction) {
      movePaths(ImmutableList.of(new MoveCommand(srcFs, src, dstFs, dst)), group, commitAction);
  }

  /**
   * Move a set of {@link Path}.
   *
   * <p>
   *   This method submits a task to move a set of {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param commands the set of move commands
   * @param group an optional group name for the destination path
   * @param commitAction an action to perform when the move completes successfully
   */
  public void movePaths(final List<MoveCommand> commands, final Optional<String> group, final Action commitAction) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        for (MoveCommand command : commands) {
          log.info(String.format("Moving %s to %s", command.src, command.dst));
          Lock lock = locks.get(command.src.toString());
          lock.lock();
          try {
            if (command.srcFs.exists(command.src)) {
              HadoopUtils.movePath(command.srcFs, command.src, command.dstFs, command.dst);
              if (group.isPresent()) {
                HadoopUtils.setGroup(command.dstFs, command.dst, group.get());
              }
            }
          } catch (FileAlreadyExistsException e) {
            LOGGER.warn(String.format("Failed to move %s to %s: dst already exists", command.src, command.dst), e);
            return null;
          } finally {
            lock.unlock();
          }
        }
        if (commitAction != null) {
          commitAction.apply();
        }
        return null;
      }
    }));
  }

  @Override
  public void close() throws IOException {
    try {
      // Wait for all submitted tasks to complete
      for (Future<?> future : this.futures) {
        future.get();
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ExecutionException ee) {
      throw new IOException(ee.getCause());
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(LOGGER));
    }
  }

  public static class MoveCommand {
    private FileSystem srcFs;
    private Path src;
    private FileSystem dstFs;
    private Path dst;

    public MoveCommand(FileSystem srcFs, Path src, FileSystem dstFs, Path dst) {
      this.srcFs = srcFs;
      this.src = src;
      this.dstFs = dstFs;
      this.dst = dst;
    }
  }
}
